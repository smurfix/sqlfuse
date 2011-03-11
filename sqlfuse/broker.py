# -*- coding: utf-8 -*-

#    Copyright (C) 2010,2011  Matthias Urlichs <matthias@urlichs.de>
#
#    This program may be distributed under the terms of the GNU GPLv3.
#
## This file is formatted with tabs.
## Do NOT introduce leading spaces.

from __future__ import division, print_function, absolute_import

__all__ = ('SQLserver','NodeChecker', 'build_response', 'InvalidResponse')

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

class SQLrealm(object):
	implements(portal.IRealm)
	def __init__(self,filesystem):
		self.filesystem = filesystem

#	def requestAvatar(self, avatarID, mind, *interfaces):
#		assert pb.IPerspective in interfaces
#		avatar = SQLnode(avatarID, self.filesystem)
#		avatar.attached(mind)
#		return pb.IPerspective, avatar, lambda a=avatar:a.detached(mind)

class SQLnode(pb.Avatar,pb.Referenceable):
	"""\
		This is the avatar, i.e. the server-side node which receives requests
		on behalf of a client
		"""
	def __init__(self, realm, node_id):
		self.node = node_id
		self.filesystem = realm.filesystem
	def setup(self):
		"""\
			Do any possibly-blocking setup stuff here.
			"""
		pass

	def attached(self, mind):
		self.remote = mind
	def detached(self, mind):
		self.remote = None

	def perspective_joinGroup(self, groupname, allowMattress=True):
		return self.server.joinGroup(groupname, self, allowMattress)
	def send(self, message):
		self.remote.callRemote("print", message)


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

class SQLportal(object):
	implements(pb.IPBRoot)
	def __init__(self, realm):
		self.realm = realm

	def rootObject(self, broker):
		return _Login1Wrapper(self.realm, broker)

class _Login1Wrapper(object,pb.Referenceable):
	def __init__(self, realm,broker):
		self.realm = realm
		self.broker = broker

	def remote_login1(self, src,dest,c):
		if not isinstance(src,int) or not isinstance(dest,int) or not isinstance(c,str):
			return failure.Failure(ValueError("login params"))
		self.src = src
		self.dest = dest
		fs = self.realm.filesystem
		if dest != fs.node_id or src == fs.node_id:
			return failure.Failure(WrongNode())
		db = self.realm.filesystem.db()
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
	def __init__(self, old,c):
		self.realm = old.realm
		self.broker = old.broker
		self.src = old.src
		self.dest = old.dest
		self.challenge = c
	def remote_login2(self,res,avatar):
		db = self.realm.filesystem.db()
		d = db.DoFn("select secret from node where id=${node}", node=self.dest)
		def cbsj2(r):
			r, = r
			if res != build_response(self.challenge,r):
				return failure.Failure(error.UnauthorizedLogin())
			self.node = SQLnode(self.realm, self.src)
			return self.node.setup()
		def cbsj3(r):
			return self.node
		d.addCallback(cbsj2)
		d.addBoth(db.rollback)
		d.addCallback(cbsj3)
		d.addErrback(log.err)
		return d


class SQLserver(object):
	def __init__(self, filesystem):
		self.filesystem = filesystem
		realm = SQLrealm(self.filesystem)
		checker = NodeChecker(self.filesystem.node_id)
		p = SQLportal(realm)
		self.listener = reactor.listenTCP(self.filesystem.port, pb.PBServerFactory(p))


