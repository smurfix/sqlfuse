# -*- coding: utf-8 -*-

#    Copyright (C) 2010,2011  Matthias Urlichs <matthias@urlichs.de>
#
#    This program may be distributed under the terms of the GNU GPLv3.
#
## This file is formatted with tabs.
## Do NOT introduce leading spaces.

from __future__ import division, print_function, absolute_import

__all__ = ('SqlClientFactory','NodeCredentials',)

"""\
This module handles the client side of connecting SqlFuse nodes.
"""

from zope.interface import implements

from twisted.cred import credentials,error
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks,returnValue
from twisted.spread import pb
from twisted.python import log

from sqlfuse.broker import build_response,InvalidResponse

#		Twisted normally does its authorization dance with a couple of
#		classes, adapters, and whatnot. This way, however, is at least
#		understandable for peole who don't live and breathe the stuff.

class NodeCredentials(object):
	"""\
		This object represents the client-side credentials storage.
		"""

	implements(credentials.ICredentials)
	def __init__(self, db, src, dest):
		self.db = db
		self.src = src
		self.dest = dest
		self.challenge = None
	def new_challenge(self):
		self.challenge = open("/dev/urandom","r").read(64)

class TestClient(object,pb.Referenceable):
	"""\
		This is the "mind" object, i.e. which initially receives remote
		requests from the server.
		"""
	def __init__(self,filesystem):
		self.filesystem = filesystem

	def remote_echo(self, message):
		print("ECHOS",message)
		return message

class SqlClientFactory(pb.PBClientFactory,object):
	def __init__(self, filesystem):
		super(SqlClientFactory,self).__init__(True)
		self.filesystem = filesystem

	def _cbSendInfo1(self, root, creds, node):
		creds.new_challenge()
		d = root.callRemote("login1", creds.src,creds.dest, creds.challenge)
		d.addCallback(self._cbSendInfo2, creds, node)
		return d

	def _cbSendInfo2(self, res, creds, node):
		(root2,challenge,res) = res
		db = self.filesystem.db()
		d = db.DoFn("select secret from node where id=${node}", node=creds.src)

		def cbsi1(r):
			r, = r
			if build_response(creds.challenge,r) != res:
				# Yes, we also authorize the server to the client.
				return failure.Failure(error.UnauthorizedLogin())
			return db.DoFn("select secret from node where id=${node}", node=creds.dest)
		def cbsi2(r):
			r, = r
			return root2.callRemote("login2", build_response(challenge,r), node)
		d.addCallback(cbsi1)
		d.addBoth(db.rollback)
		d.addCallback(cbsi2)
		d.addErrback(log.err)
		return d
		

	def login(self, credentials, node):
		"""
		Login and get perspective from remote PB server.

		@rtype: L{Deferred}
		@return: A L{Deferred} which will be called back with a
			L{RemoteReference} for the avatar logged in to, or which will
			errback if login fails.
		"""
		d = self.getRootObject()

		d.addCallback(self._cbSendInfo1, credentials, node)
		return d


class TestConnector(object):
	def __init__(self,filesystem,node_id):
		super(TestConnector,self).__init__()
		self.filesystem = filesystem
		self.node_id = node_id

	@inlineCallbacks
	def connect(self):
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
			reactor.connectTCP(adr,port, factory)
			res = yield factory.login(NodeCredentials(self.filesystem.db, self.filesystem.node_id,self.node_id), node=TestClient(self.filesystem))
			res = yield self.connected(res)
			returnValue (res )
		except Exception:
			log.err()

	def disconnected(self,p):
		print("disconnected",p,p.__dict__)
		
	def connected(self, perspective):
		# this perspective is a reference to our User object.  Save a reference
		# to it here, otherwise it will get garbage collected after this call,
		# and the server will think we logged out.
		perspective.notifyOnDisconnect(self.disconnected)
		return perspective

## simple test code
if __name__ == '__main__':
	class Dummy(object): pass

	from sqlmix.twisted import DbPool
	import twist

	fs = Dummy()
	fs.db = DbPool(username='test',password='test',database='test_sqlfuse',host='sql.intern.smurf.noris.de')
	fs.node_id = 3

	r = TestConnector(fs,1).connect()
	def logme(r,i):
		print(i,r)
		return r

	# r.addBoth(logme,"DID")
	def rem(r):
		r.notifyOnDisconnect(lambda _: reactor.stop())
		d = r.callRemote("echo","Bla Fasel")
		d.addBoth(logme,"ECHO")
	r.addCallback(rem)
	reactor.run()

