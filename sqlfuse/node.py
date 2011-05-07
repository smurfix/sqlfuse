# -*- coding: utf-8 -*-

#    Copyright (C) 2010,2011  Matthias Urlichs <matthias@urlichs.de>
#
#    This program may be distributed under the terms of the GNU GPLv3.
#
## This file is formatted with tabs.
## Do NOT introduce leading spaces.

from __future__ import division, absolute_import

"""\
This module exports a SqlNode, i.e. an object which represents another
SqlFuse node.

Right now, only native interconnection is supported.

"""

__all__ = ('SqlNode','DataMissing','NoConnection')

import os

from zope.interface import implements

from twisted.python import log,failure
from twisted.spread import pb
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet import error as err

from sqlfuse import trace,tracer_info, triggeredDefer, NoLink
from sqlfuse.connect import INode
from sqlfuse.fs import SqlInode,DB_RETRIES
from sqlmix import NoData

INITIAL_RETRY=1
MAX_RETRY=60
MAX_BLOCK=65536
ECHO_TIMER=1
ECHO_TIMEOUT=10

tracer_info['remote']="Interaction between nodes"

class NoConnection(RuntimeError):
	"""\
		There's no connection to a remote node.
		"""
	pass

class DataMissing(BufferError):
	"""\
		A read callback didn't get all the data, because the local cache
		didn't have them.
		"""
	pass

class SqlNode(pb.Avatar,pb.Referenceable):
	"""\
		This is the node (both server- and client-side) which receives
		requests on behalf of a client.
		"""
	implements(INode)

	retry_timeout = INITIAL_RETRY
	retry_timer = None

	def __init__(self, filesystem, node_id):
		self.node_id = node_id
		self.fs = filesystem
		self.echo_timer = None
		self._connector = None
		self._server = None
		self._clients = set()

		self.queue_retry()

	def connect_timer(self):
		self.retry_timer = None

		if self._server or self._connector or self.node_id is None:
			if self.node_id:
				trace('remote',"connect_timer: inprogress to node %d",self.node_id)
			return
		d = self.connect()
		def grab_nolink(r):
			r.trap(NoLink)
			trace('remote',"connect to node %d failed",self.node_id)
			self.queue_retry()
		d.addErrback(grab_nolink)
		d.addErrback(lambda r: log.err(r,"Connection timer"))

	def connect_retry(self):
		"""\
			Try to connect; returns a Deferred which contains this
			attempt's state. However, on failure also install a retry
			timer.
			"""
		if self.retry_timer:
			self.retry_timer.cancel()
			self.retry_timer = None
		d = self.connect()
		def retrier(r):
			self.queue_retry()
			trace('remote',"Error: will retry %d",self.node_id)
			return r
		d.addErrback(retrier)
		def rep(r):
			trace('remote',"connect_retry: no error to %d",self.node_id)
		d.addCallback(rep)
		return d

	@inlineCallbacks
	def connect(self):
		"""\
			Try to connect to this node's remote side.
			"""
		assert self.node_id is not None
		if self._server:
			return # already done
		if self._connector: # in progress: wait for it
			trace('remote',"Chain connect to node %d",self.node_id)
			yield triggeredDefer(self._connector)
			return
		trace('remote',"Connecting to node %d",self.node_id)
		try:
			with self.fs.db() as db:
				try:
					m, = yield db.DoFn("select method from updater where src=${src} and dest=${dest}",src=self.fs.node_id,dest=self.node_id)
				except NoData:
					raise NoLink(self.node_id)
				m = __import__("sqlfuse.connect."+m, fromlist=('NodeClient',))
			m = m.NodeClient(self)
			self._connector = m.connect()
			# Do this to avoid having a single Deferred both in the inline
			# callback chain and as a possible cancellation point
			yield triggeredDefer(self._connector)
			if self._server is None:
				raise NoLink(self.node_id)
		except NoLink:
			raise
		except Exception as e: # no connection
			if isinstance(e,(err.ConnectionRefusedError,NoConnection)):
				trace('remote',"No link to %d, retrying",self.node_id)
			else:
				f = failure.Failure()
				log.err(f,"Connecting remote")
			self.queue_retry()
		finally:
			self._connector = None

	def queue_retry(self):
		if self.retry_timer or not self.node_id:
			return
		self.retry_timeout *= 1.3
		if self.retry_timeout > MAX_RETRY:
			self.retry_timeout = MAX_RETRY
		self.retry_timer = reactor.callLater(self.retry_timeout, self.connect_timer)

	def disconnect_retry(self):
		if self._server:
			self._server.disconnect()
			self._server = None
		if not self._connector:
			self.queue_retry()

	def server_no_echo(self):
		self.echo_timer = None
		self.disconnect_retry()
		trace('remote',"Echo timeout")

	def server_echo(self):
		self.echo_timer = reactor.callLater(ECHO_TIMEOUT,self.server_no_echo)

		def get_echo(r):
			if self.echo_timer:
				self.echo_timer.cancel()
			self.echo_timer = reactor.callLater(ECHO_TIMER,self.server_echo)

		def get_echo_error(r):
			if self.echo_timer:
				self.echo_timer.cancel()
				self.echo_timer = None
			log.err(r,"Echo")
			self.disconnect_retry()

		d = self.do_echo("ping")
		d.addCallbacks(get_echo,get_echo_error)
			

	def server_connected(self, server):
		if self._server is not None and self._server is not server:
			self._server.disconnect()
		trace('remote',"Connected server to %d",self.node_id)
		self._server = server
		self.retry_timeout = INITIAL_RETRY

		if self.echo_timer is not None:
			self.echo_timer.cancel()
		self.echo_timer = reactor.callLater(ECHO_TIMER,self.server_echo)
		self.fs.copier.trigger()

	def server_disconnected(self, server):
		if self._server is server:
			self._server = None
			if self.echo_timer is not None:
				self.echo_timer.cancel()
				self.echo_timer = None
			trace('remote',"Disconnected server to %d",self.node_id)
			queue_retry()

	def client_connected(self, client):
		trace('remote',"Connected client from %d",self.node_id)
		self._clients.add(client)

	def client_disconnected(self, client):
		try: self._clients.remove(client)
		except (ValueError,KeyError): pass
		else:
			trace('remote',"Disconnected client from %d",self.node_id)


	def disconnect(self):
		"""\
			Drop this node: disconnect
			"""
		self.node_id = None
		if self.echo_timer:
			self.echo_timer.cancel()
			self.echo_timer = None
		r = self._server
		if r is not None:
			self._server = None
			r.disconnect()

		r = self._connector
		if r is not None:
			self._connector = None
			r.cancel()

		r = self._clients
		if r:
			self._clients = []
			for c in r:
				c.disconnect()

	def remote_echo(self,caller,msg):
		return msg
	
	def remote_exec(self,node,name,*a,**k):
		if node not in self.fs.topology:
			trace('remote',"NoLink remote %s %s %s %s %s",caller,node,name,repr(a),repr(k))
			raise NoLink(node)

		# TODO: cache calls to stuff like reading from a file
		# TODO: prevent cycles
		return self.fs.call_node(node,name,*a,**k)

	@inlineCallbacks
	def remote_readfile(self,caller,inum,reader,missing):
		inode = SqlInode(self.fs,inum)
		yield self.fs.db(inode._load, DB_RETRIES)
		if not inode.inum:
			trace('remote',"Inode %d probably deleted",inum)
			raise DataMissing(missing)

		avail = inode.cache.available & missing
		if avail:
			missing -= avail
			h = yield inode.open(os.O_RDONLY)
			def split(av):
				for a,b,c in av:
					while b > MAX_BLOCK:
						yield a,MAX_BLOCK
						a += MAX_BLOCK
						b -= MAX_BLOCK
					yield a,b
			for a,b in split(avail):
				try:
					data = yield h.read(a,b)
				except Exception as e:
					break
				try:
					yield reader.callRemote("data",a,data)
				except Exception as e:
					break
			h.release()
		if missing:
			trace('remote',"Missing: %s for %s / %s", missing,caller,reader)
			raise DataMissing(missing)
		
	

	@classmethod
	def remote_names(cls):
		for name in dir(cls):
			if name.startswith('remote_'):
				yield name[7:]

	@classmethod
	def _final_setup(cls):
		def make_callout(name):
			def _callout(self, *a,**k):
				d = self.connect()
				def check_link(r):
					if self._server is None:
						# TODO: this really should not happen
						raise NoLink(self.node_id)
				d.addCallback(check_link)
				d.addCallback(lambda r: getattr(self._server,"do_"+name)(self,*a,**k))
				return d
			return _callout
			
		for name in cls.remote_names():
			if not hasattr(cls,"do_"+name):
				setattr(cls,"do_"+name,make_callout(name))

SqlNode._final_setup()
