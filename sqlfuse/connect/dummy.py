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

__all__ = ('NodeClient','NodeServer','NodeServerFactory')

from twisted.python import log
from twisted.spread import pb
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks

from sqlfuse.connector import SqlClientFactory,NodeCredentials

class NodeClient(object):
	def __init__(self, filesystem, node_id):
		self.node = node_id
		self.fs = filesystem

	def connect(self):
		raise NotImplementedError()
	def connected(self, client=None):
		raise NotImplementedError()
	def disconnect(self):
		pass
	def disconnected(self, client):
		pass

class NodeServer(object):
	def __init__(self):
		pass
	def disconnect(self):
		pass

class NodeServerFactory(object):
	def __init__(self,fs):
		pass
	def connect(self):
		raise NotImplementedError()
	def disconnect(self):
		pass
		
