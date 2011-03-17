# -*- coding: utf-8 -*-

#    Copyright (C) 2010,2011  Matthias Urlichs <matthias@urlichs.de>
#
#    This program may be distributed under the terms of the GNU GPLv3.
#
## This file is formatted with tabs.
## Do NOT introduce leading spaces.

from __future__ import division, print_function, absolute_import

"""\
This module handles the native connection method.
"""

__all__ = ('NodeClient','NodeServer','NodeServerFactory')

from zope.interface import implements

from twisted.cred import portal, checkers, credentials, error
from twisted.internet import reactor
from twisted.internet import error as err
from twisted.internet.defer import inlineCallbacks
from twisted.manhole import service
from twisted.python import log
from twisted.python.hashlib import md5
from twisted.spread import pb

from sqlfuse.connect import INodeClient,INodeServer,INodeServerFactory
from sqlfuse.node import SqlNode

class InvalidResponse(error.UnauthorizedLogin):
	"""You didn't provide the correct response to the challenge."""
	pass

class WrongNode(error.LoginFailed):
	"""You connected to the wrong destination. Shame on you."""
	pass

class UnknownSource(error.LoginFailed):
	"""The source node you provided was unknown."""
	pass


def build_response(challenge, password):
	"""\
		Respond to a challenge.

		This is useful for challenge/response authentication.
		"""
	m = md5()
	m.update(password)
	hashedPassword = m.digest()
	m = md5()
	m.update(hashedPassword)
	m.update(challenge)
	doubleHashedPassword = m.digest()
	return doubleHashedPassword


###############
## Interface ##
###############

# This code uses one interface node for both directions.

class NodeAdapter(pb.Avatar,pb.Referenceable):
	implements(INodeClient,INodeServer)

	_connector = None

	def __init__(self,node,proxy=None):
		self.node = node
		self.proxy = proxy
	
	@inlineCallbacks
	def connect(self):
		"""\
			The node tells me to connect to its remote side.
			"""
		try:
			fs = self.node.filesystem
			with fs.db() as db:
				conn, = yield db.DoFn("select args from updater where src=${src} and dest=${dest} and method='native'",src=fs.node_id,dest=self.node.node_id)
				conn = eval(conn)
				if len(conn) > 1:
					port = conn[1]
				else:
					port, = yield db.DoFn("select port from node where id=${node}", node=self.node.node_id)
					assert port > 0
			adr = conn[0]
			factory = SqlClientFactory(self,fs)
			self._connector = reactor.connectTCP(adr,port, factory)
			res = yield factory.login(NodeCredentials(fs.db, fs.node_id,self.node.node_id))
			self._connector = None
			self.connected(res)
		except err.ConnectionRefusedError:
			raise
		except Exception:
			log.err()
			raise
		
	def connected(self, proxy):
		"""\
			The other side tells me that it has connected.
			"""
		self.proxy = proxy
		proxy.notifyOnDisconnect(self.disconnected)

		if self._connector:
			self._connector.disconnect()
			self._connector = None

		self.node.client_connected(self)
		try:
			self.node.server_connected(self)
		except BaseException:
			self.node.client_disconnected(self)
			raise

	def disconnect(self):
		"""\
			Terminate my connection and die.
			This is called by the node.
			Thus, we don't call back to the node here.
			"""
		if self.node is None:
			return # may be duplicate, since we're both client and server

		if self._connector: # trying to connect?
			self._connector.disconnect()
			self._connector = None

		if self.proxy: # connected?
			self.proxy.transport.loseConnection()
			self.proxy = None
		self.node = None

	def disconnected(self,what=None):
		"""\
			The connection to the remote side is gone.
			This is called by the remote adapter.
			"""
		self.node.client_disconnected(self)
		self.node.server_disconnected(self)


##############
## INodeClient
##############

#	def do_echo(self,data):
#		return self.proxy.callRemote("echo",data)


##############
## INodeServer
##############

#	def remote_echo(self,data):
#		return self.node.remote_echo(data)


#################
## Client side ##
#################

class SqlClientFactory(pb.PBClientFactory,object):
	def __init__(self, node, filesystem):
		super(SqlClientFactory,self).__init__(True)
		self.node = node
		self.filesystem = filesystem

	def login(self, credentials):
		"""
		Login and get perspective from remote PB server.

		@rtype: L{Deferred}
		@return: A L{Deferred} which will be called back with a
			L{RemoteReference} for the avatar logged in to, or which will
			errback if login fails.
		"""
		d = self.getRootObject()

		d.addCallback(self._cbSendInfo1, credentials)
		return d

	def _cbSendInfo1(self, root, creds):
		creds.new_challenge()
		d = root.callRemote("login1", creds.src,creds.dest, creds.challenge)
		d.addCallback(self._cbSendInfo2, creds)
		return d

	def _cbSendInfo2(self, res, creds):
		(root2,challenge,res) = res
		db = self.node.node.filesystem.db()
		d = db.DoFn("select secret from node where id=${node}", node=creds.src)

		def cbsi1(r):
			r, = r
			if build_response(creds.challenge,r) != res:
				# Yes, we also authorize the server to the client.
				return failure.Failure(error.UnauthorizedLogin())
			return db.DoFn("select secret from node where id=${node}", node=creds.dest)
		def cbsi2(r):
			r, = r
			return root2.callRemote("login2", build_response(challenge,r), self.node)
		d.addCallback(cbsi1)
		d.addBoth(db.rollback)
		d.addCallback(cbsi2)
		d.addErrback(log.err)
		return d
		

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


#################
## Server side ##
#################

class _Portal(object):
	"""\
		The portal's main function is to return an initial root object,
		i.e. the node which the client initially gets after connecting.

		Obviously, the only thing the client should do at this point is to
		log in ...
		"""
	implements(pb.IPBRoot)
	def __init__(self, portal, filesystem):
		self.manhole_portal = portal
		self.filesystem = filesystem

	def rootObject(self, broker):
		"""\
			... so we return an object which supports exactly that.
			"""
		return _Login1Wrapper(self.manhole_portal, self.filesystem, broker)

class _Login1Wrapper(object,pb.Referenceable):
	"""\
		First step when logging in a client.
		"""
	def __init__(self, portal, filesystem,broker):
		self.manhole_portal = portal
		self.filesystem = filesystem
		self.broker = broker
		self.dead = False

	def remote_login(self, username):
		"""\
			This is the login method for twisted.manhole connections.
			"""
		if self.manhole_portal is None:
			raise NotImplementedError("This server doesn't have manhole access")
		c = open("/dev/urandom","r").read(63)
		return c, _ManholeChallenger(self.manhole_portal, self.broker, username, c)

	def remote_login1(self, src,dest,c):
		"""\
			This is the login method for server↔server connections.
			The client supplies source and destination node IDs, and a
			challenge string for authorizing the server to the client.

			The server replies with a hash of its own secret,
			challenge for authorizing the client, and a node for the second
			step of the process.
			"""
		if self.dead:
			return failure.Failure(RuntimeError("you already tried this. Shame on you."))
		self.dead = True

		if not isinstance(src,int) or not isinstance(dest,int) or not isinstance(c,str):
			return failure.Failure(ValueError("login params"))
		if len(c) < 16:
			return failure.Failure(ValueError("your challenge string is pathetic"))

		self.src = src
		self.dest = dest
		fs = self.filesystem
		if dest != fs.node_id or src == fs.node_id:
			return failure.Failure(WrongNode())
		db = fs.db()
		d = db.DoFn("select secret from node where id=${node}", node=self.src)
		d.addBoth(db.rollback)
		def cbsi1(r):
			r, = r
			nc = open("/dev/urandom","r").read(63)
			return (_Login2Wrapper(self,nc),nc, build_response(c,r))
		d.addCallback(cbsi1)
		d.addErrback(log.err)
		return d

class _ManholeChallenger(pb.Referenceable, pb._JellyableAvatarMixin):
	"""
	Called with response to password challenge.
	"""
	implements(credentials.IUsernameHashedPassword, pb.IUsernameMD5Password)

	def __init__(self, portal, broker, username, challenge):
		self.portal = portal
		self.broker = broker
		self.username = username
		self.challenge = challenge

	def remote_respond(self, response, mind):
		self.response = response
		d = self.portal.login(self, mind, pb.IPerspective)
		d.addCallback(self._cbLogin)
		return d

	# IUsernameHashedPassword:
	def checkPassword(self, password):
		return self.checkMD5Password(md5(password).digest())

	# IUsernameMD5Password
	def checkMD5Password(self, md5Password):
		md = md5()
		md.update(md5Password)
		md.update(self.challenge)
		correct = md.digest()
		return self.response == correct

class _Login2Wrapper(object,pb.Referenceable):
	"""\
		This node controls the second step of the login process.
		"""
	def __init__(self, old,c):
		self.filesystem = old.filesystem
		self.broker = old.broker
		self.src = old.src
		self.dest = old.dest
		self.challenge = c
		self.dead = False
	def remote_login2(self,res,avatar):
		"""\
			The client sends its answer to the server's challenge, and a way to
			talk to it (if necessary). The server replies with (a reference
			to) its avatar.
			"""
		if self.dead:
			return failure.Failure(RuntimeError("you already tried this. Shame on you."))
		self.dead = True

		db = self.filesystem.db()
		d = db.DoFn("select secret from node where id=${node}", node=self.dest)
		def cbsj2(r):
			r, = r
			if res != build_response(self.challenge,r):
				return failure.Failure(error.UnauthorizedLogin())
			self.node = NodeAdapter(self.filesystem.remote[self.src])
			return self.node.connected(avatar)
		#def cbsj3(r):
			#dd = avatar.callRemote("echo","baz xyzzy")
			#def cbl(r):
			#	print("ECHOS",r)
			#dd.addBoth(cbl)
			#return dd
		d.addCallback(cbsj2)
		d.addBoth(db.rollback)
		#d.addCallback(cbsj3)
		d.addCallback(lambda _: self.node)
		d.addErrback(log.err)
		return d

NodeServer = NodeAdapter
NodeClient = NodeAdapter
def _build_callout(name):
	def _callout(self,*a,**k):
		print("CALLOUT",name,a,k)
		return self.proxy.callRemote(name,*a,**k)
	return _callout
def _build_callin(name):
	def _callin(self,*a,**k):
		print("CALLIN",name,a,k)
		return getattr(self.node,"remote_"+name)(*a,**k)
	return _callin
for name in SqlNode.remote_names():
	if not hasattr(NodeAdapter,"remote_"+name):
		setattr(NodeAdapter,"remote_"+name,_build_callin(name))
	if not hasattr(NodeAdapter,"do_"+name):
		setattr(NodeAdapter,"do_"+name,_build_callout(name))


class NodeServerFactory(object):
	implements(INodeServerFactory)

	def __init__(self, filesystem):
		self.filesystem = filesystem
	
	@inlineCallbacks
	def connect(self):
		ns = {}

		from sqlfuse import fs
		ns["fs"] = self.filesystem
		ns["inodes"] = fs._Inode

		user="admin"
		with self.filesystem.db() as db:
			password,= yield db.DoFn("select `password` from node where id=${node}", node=self.filesystem.node_id)
		if password:
			mp = portal.Portal( service.Realm(service.Service(True,ns)),
			    [checkers.InMemoryUsernamePasswordDatabaseDontUse(**{user: password})])
		else:
			mp = None

		p = _Portal(mp, self.filesystem)
		self.listener = reactor.listenTCP(self.filesystem.port, pb.PBServerFactory(p))
	
	def disconnect(self):
		if self.listener is not None:
			self.listener.stopListening()
			self.listener = None

