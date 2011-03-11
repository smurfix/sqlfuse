# -*- coding: utf-8 -*-

#    Copyright (C) 2010,2011  Matthias Urlichs <matthias@urlichs.de>
#
#    This program may be distributed under the terms of the GNU GPLv3.
#
## This file is formatted with tabs.
## Do NOT introduce leading spaces.

from __future__ import division, print_function, absolute_import

__all__ = ('SQLconnector',)

from zope.interface import implements

from twisted.cred import credentials,error
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks,returnValue
from twisted.spread import pb
from twisted.python import log

from sqlfuse.broker import build_response,InvalidResponse


class NodeCredentials(object):
	implements(credentials.ICredentials)
	def __init__(self, db, src, dest):
		self.db = db
		self.src = src
		self.dest = dest
		self.challenge = None
	def new_challenge(self):
		self.challenge = open("/dev/urandom","r").read(64)

class SQLclient(object,pb.Referenceable):
	"""\
		This is the "mind" object, i.e. which initially receives remote
		requests from the server.
		"""
	def __init__(self,filesystem):
		self.filesystem = filesystem

class SQLclientFactory(pb.PBClientFactory,object):
	def __init__(self, filesystem):
		super(SQLclientFactory,self).__init__(True)
		self.filesystem = filesystem

	def _cbSendInfo1(self, root, creds, client):
		creds.new_challenge()
		d = root.callRemote("login1", creds.src,creds.dest, creds.challenge)
		d.addCallback(self._cbSendInfo2, creds, client)
		return d

	def _cbSendInfo2(self, res, creds, client):
		(root2,challenge,res) = res
		db = self.filesystem.db()
		d = db.DoFn("select secret from node where id=${node}", node=creds.src)

		def cbsi1(r):
			r, = r
			if build_response(creds.challenge,r) != res:
				return failure.Failure(error.UnauthorizedLogin())
			return db.DoFn("select secret from node where id=${node}", node=creds.dest)
		def cbsi2(r):
			r, = r
			return root2.callRemote("login2", build_response(challenge,r), SQLclient(self.filesystem))
		d.addCallback(cbsi1)
		d.addBoth(db.rollback)
		d.addCallback(cbsi2)
		d.addErrback(log.err)
		return d
		

	def login(self, credentials, client):
		"""
		Login and get perspective from remote PB server.

		@rtype: L{Deferred}
		@return: A L{Deferred} which will be called back with a
			L{RemoteReference} for the avatar logged in to, or which will
			errback if login fails.
		"""
		d = self.getRootObject()

		d.addCallback(self._cbSendInfo1, credentials, client)
		return d


class Client(object):
	def __init__(self,filesystem,node_id):
		super(Client,self).__init__()
		self.filesystem = filesystem
		self.node_id = node_id

	def remote_print(self, message):
		print(message)

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
			factory = SQLclientFactory(self.filesystem)
			reactor.connectTCP(adr,port, factory)
			res = yield factory.login(NodeCredentials(self.filesystem.db, self.filesystem.node_id,self.node_id), client=SQLclient)
			yield self.connected(res)
		except Exception:
			log.err()



	def connected(self, perspective):
		print("connected",perspective)
		# this perspective is a reference to our User object.  Save a reference
		# to it here, otherwise it will get garbage collected after this call,
		# and the server will think we logged out.
		reactor.stop()

if __name__ == '__main__':
	class Dummy(object): pass

	from sqlmix.twisted import DbPool
	import twist

	fs = Dummy()
	fs.db = DbPool(username='test',password='test',database='test_sqlfuse',host='sql.intern.smurf.noris.de')
	fs.node_id = 3

	Client(fs,1).connect()
	reactor.run()

