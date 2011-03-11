# -*- coding: utf-8 -*-

#    Copyright (C) 2010,2011  Matthias Urlichs <matthias@urlichs.de>
#
#    This program may be distributed under the terms of the GNU GPLv3.
#
## This file is formatted with tabs.
## Do NOT introduce leading spaces.

from __future__ import division, print_function, absolute_import

"""\
This module exports a SqlNode, i.e. an object which represents another
SqlFuse node.

Right now, only native interconnection is supported.

"""

__all__ = ('SqlNode',)

from zope.interface import implements

from twisted.python import log
from twisted.spread import pb
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks,Deferred
from twisted.internet import error as err

from sqlfuse.connect import INode
from sqlmix import NoData

INITIAL_RETRY=1
MAX_RETRY=60

class NoLink(RuntimeError):
	pass

class SqlNode(pb.Avatar,pb.Referenceable):
	"""\
		This is the node (both server- and client-side) which receives
		requests on behalf of a client.
		"""
	implements(INode)
	def __init__(self, filesystem, node_id):
		self.node_id = node_id
		self.filesystem = filesystem
		self.retry_timeout = INITIAL_RETRY
		self.retry_timer = reactor.callLater(INITIAL_RETRY, self.connect_timer)
		self._connector = None

		self._server = None
		self._clients = set()

	def connect_timer(self):
		self.retry_timer = None
		if self._server or self._connector:
			return
		self.connect()

	@inlineCallbacks
	def connect(self):
		"""\
			Try to connect to this node's remote side.
			"""
		if self._server:
			return # already done
		if self._connector: # in progress: wait for it
			d = Deferred()
			self._connector.chainDeferred(d)
			yield d
			return
		try:
			with self.filesystem.db() as db:
				try:
					m, = yield db.DoFn("select method from updater where src=${src} and dest=${dest}",src=self.filesystem.node_id,dest=self.node_id)
				except NoData:
					raise NoLink
				m = __import__("sqlfuse.connect."+m, fromlist=('NodeClient',))
			m = m.NodeClient(self)
			self._connector = m.connect()

			# Do this to avoid having a single Deferred both in the inline
			# callback chain and as a possible cancellation point
			d = Deferred()
			self._connector.chainDeferred(d)
			yield d
		except NoLink:
			raise
		except Exception as e: # no connection
			if isinstance(e,err.ConnectionRefusedError):
				pass
			else:
				log.err()
			self.retry_timeout *= 1.3
			if self.retry_timeout > MAX_RETRY:
				self.retry_timeout = MAX_RETRY
			self.retry_timer = reactor.callLater(self.retry_timeout, self.connect_timer)
		finally:
			self._connector = None


	def server_connected(self, server=None):
		if self._server is not None and self._server is not server:
			self._server.disconnect()
		self._server = server

	def server_disconnected(self, server=None):
		if self._server is server:
			self._server = None
			if self.node_id is not None:
				self.retry_timer = reactor.callLater(INITIAL_RETRY, self.connect_timer)

	def client_connected(self, client=None):
		self._clients.add(client)

	def client_disconnected(self, client=None):
		try: self._clients.remove(client)
		except KeyError: pass


	def disconnect(self):
		"""\
			Drop this node: disconnect
			"""
		self.node_id = None
		r = self._server
		if r is not None:
			self._server = None
			r.transport.loseConnection()
		r = self._connector
		if r is not None:
			self._connector = None
			r.cancel()

	def remote_echo(self,msg):
		return msg


	@classmethod
	def remote_names(cls):
		for name in dir(cls):
			if name.startswith('remote_'):
				yield name[7:]

	@classmethod
	def _final_setup(cls):
		def make_callout(name):
			def _callout(self, *a,**k):
				return getattr(self._server,name)(self,*a,**k)
			return _callout
			
		for name in cls.remote_names():
			if not hasattr(cls,"do_"+name):
				setattr(cls,"do_"+name,make_callout(name))

SqlNode._final_setup()
