# -*- coding: utf-8 -*-

#    Copyright (C) 2010,2011  Matthias Urlichs <matthias@urlichs.de>
#
#    This program may be distributed under the terms of the GNU GPLv3.
#
## This file is formatted with tabs.
## Do NOT introduce leading spaces.

from __future__ import division, print_function, absolute_import

"""\
This module handles the server side of connecting SqlFuse nodes.
"""

__all__ = ('SqlServer','NodeChecker', 'build_response', 'InvalidResponse')

from zope.interface import implements

from twisted.cred import portal, checkers, credentials, error
from twisted.internet import reactor
from twisted.python import log
from twisted.python.hashlib import md5
from twisted.spread import pb

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
    """Respond to a challenge.

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


class IHashedSecretAuth(credentials.ICredentials):

	#challenge
	#node_id
	def checkResponse(response):
		"""
		Validate that the response matches the challenge for this node.
		"""

class NodeChecker(object):
	implements(checkers.ICredentialsChecker)

	credentialInterfaces = (IHashedSecretAuth,)

	def __init__(self, node_id):
		self.node_id = node_id

	def _cbPasswordMatch(self, matched, username):
		if matched:
			return username
		else:
			return failure.Failure(error.UnauthorizedLogin())

	def requestAvatarId(self, credentials):
		import pdb;pdb.set_trace()
		if credentials.username in self.users:
			return defer.maybeDeferred(
				credentials.checkPassword,
				self.users[credentials.username]).addCallback(
				self._cbPasswordMatch, str(credentials.username))
		else:
			return defer.fail(error.UnauthorizedLogin())

class SqlPortal(object):
	"""\
		The portal's main function is to return an initial root object,
		i.e. the node which the client initially gets after connecting.

		Obviously, the only thing the client should do at this point is to
		log in ...
		"""
	implements(pb.IPBRoot)
	def __init__(self, filesystem):
		self.filesystem = filesystem

	def rootObject(self, broker):
		"""\
			... so we return an object which supports exactly that.
			"""
		return _Login1Wrapper(self.filesystem, broker)

class _Login1Wrapper(object,pb.Referenceable):
	"""\
		First step when logging in a client.
		"""
	def __init__(self, filesystem,broker):
		self.filesystem = filesystem
		self.broker = broker
		self.dead = False

	def remote_login1(self, src,dest,c):
		"""\
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
			self.node = self.filesystem.remote[self.src]
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


class SqlServer(object):
	def __init__(self, filesystem):
		self.filesystem = filesystem
		checker = NodeChecker(self.filesystem.node_id)
		p = SqlPortal(self.filesystem)
		self.listener = reactor.listenTCP(self.filesystem.port, pb.PBServerFactory(p))
	
	def disconnect(self):
		if self.listener is not None:
			self.listener.stopListening()
			self.listener = None

