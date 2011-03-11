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

from twisted.python import log
from twisted.spread import pb
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks

from sqlfuse.connector import SqlClientFactory,NodeCredentials

INITIAL_RETRY=1
MAX_RETRY=60

class SqlNode(pb.Avatar,pb.Referenceable):
	"""\
		This is the node (both server- and client-side) which receives
		requests on behalf of a client.
		"""
	def __init__(self, filesystem, node_id):
		self.node = node_id
		self.filesystem = filesystem
		self._remote = None
		self.retry_timeout = INITIAL_RETRY
		self.retry_timer = reactor.callLater(INITIAL_RETRY, self.connect_timer)
		self._connector = None

	def connect_timer(self):
		self.retry_timer = None
		self.connect()

	@inlineCallbacks
	def connect(self):
		"""\
			Try to connect to this node's remote side.
			"""
		if self._remote:
			return # already done
		try:
			with self.filesystem.db() as db:
				conn, = yield db.DoFn("select args from updater where src=${src} and dest=${dest} and method='native'",src=self.filesystem.node_id,dest=self.node_id)
				conn = eval(conn)
				if len(conn) > 1:
					port = conn[1]
				else:
					port, = yield db.DoFn("select port from node where id=${node}", node=self.node_id)
					assert port > 0
			adr = conn[0]
			factory = SqlClientFactory(self.filesystem)
			self._connector = reactor.connectTCP(adr,port, factory)
			res = yield factory.login(NodeCredentials(self.filesystem.db, self.filesystem.node_id,self.node_id), node=self)
			self._connector = None
			self.connected(res)
		except Exception:
			log.err()
			self._connector = None
			self.retry_timeout *= 1.3
			if self.retry_timeout > MAX_RETRY:
				self.retry_timeout = MAX_RETRY
			self.retry_timer = reactor.callLater(INITIAL_RETRY, self.connect_timer)

	def connected(self, client=None):
		"""\
			This is called when a remote client connects to this server.
			"""
		if self._remote is not None: # duplicate
			self._remote.transport.loseConnection()
		self._remote = client
		self.retry_timeout = INITIAL_RETRY
		client.notifyOnDisconnect(self.disconnected)

	def disconnect(self):
		"""\
			Drop this node: disconnect
			"""
		r = self._remote
		if r is not None:
			self._remote = None
			r.transport.loseConnection()
		r = self._connector
		if r is not None:
			self._connector = None
			r.disconnect()

	def disconnected(self, client):
		if self._remote == client:
			self._remote = None

	def remote_echo(self,msg):
		return msg

