# -*- coding: utf-8 -*-
from __future__ import division

##
##  Copyright © 2007-2011, Matthias Urlichs <matthias@urlichs.de>
##
##  This program is free software: you can redistribute it and/or modify
##  it under the terms of the GNU General Public License as published by
##  the Free Software Foundation, either version 3 of the License, or
##  (at your option) any later version.
##
##  This program is distributed in the hope that it will be useful,
##  but WITHOUT ANY WARRANTY; without even the implied warranty of
##  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
##  GNU General Public License (included; see the file LICENSE)
##  for more details.
##

"""\
	This module holds some Twisted hacks and (admittedly) monkey patches.
	"""

from twisted.internet import fdesc,defer,reactor,base
from twisted.internet.abstract import FileDescriptor
from twisted.python import failure,log
from twisted.python.threadable import isInIOThread
from twisted.spread import pb

from posix import write
import sys

__all__ = ("StdInDescriptor","StdOutDescriptor","deferToLater")

# nonblocking versions of stdin/stdout

class StdInDescriptor(FileDescriptor):
	def fileno(self):
		return 0
	def doRead(self):
		try:
			fdesc.setNonBlocking(0)
			return fdesc.readFromFD(0, self.dataReceived)
		finally:
			fdesc.setBlocking(0)

class StdOutDescriptor(FileDescriptor):
	def fileno(self):
		return 1
	def writeSomeData(self, data):
		try:
			fdesc.setNonBlocking(1)
			return write(1,data)
		finally:
			fdesc.setBlocking(1)
	

# count the number of active defer-to-later handlers
# so that we don't exit when one of them is still running,
# because that causes a deadlock.
_running = 0
_stopme = False

_rs = reactor.stop
def _reactorstop():
	global _stopme
	assert not _stopme , "Only one idle callback allowed"
	if _running:
		_stopme = True
	else:
		_rs()
reactor.stop = _reactorstop
def _defer(d):
	global _running
	global _stopme
	d.callback(None)
	_running -= 1
	if _stopme and not _running:
		_stopme = False
		_rs()

# also, don't require an isInIOThread() test each time we want to defer something
# from a thread which may or may not be the main one.
def deferToLater(p,*a,**k):
	global _running
	_running += 1

	d = defer.Deferred()
	d.addCallback(lambda _: p(*a,**k))
	if isInIOThread():
		reactor.callLater(0,_defer,d)
	else:
		reactor.callFromThread(_defer,d)
	reactor.wakeUp()
	return d


# Simplification: sometimes we're late starting something.
# That is not a bug, that's life.
# reactor.callLater asserts >=0, so just make sure that it is.
wcl = reactor.callLater
def wake_later(t,p,*a,**k):
	if t < 0: t = 0

	r = wcl(t,p,*a,**k)

	# Bug workaround: Sometimes the Twisted reactor seems not to notice
	# that we just called reactor.callLater().
	if reactor.waker:
		reactor.waker.wakeUp()

	return r
reactor.callLater = wake_later


# Allow a Deferred to be called with another Deferred,
# so that the result of the second is fed to the first.
# This happens when you do this:
#     d.addCallback(something_returning_a_deferred())
# which is, or should be, entirely reasonable.
_dcb = defer.Deferred.callback
def acb(self, result):
	if isinstance(result, defer.Deferred):
		result.addCallbacks(self.callback,self.errback)
	else:
		_dcb(self,result)
defer.Deferred.callback = acb


# Always encode Unicode strings in utf-8
fhw = FileDescriptor.write
def nfhw(self,data):
	if isinstance(data,unicode):
		data = data.encode("utf-8")
	return fhw(self,data)
FileDescriptor.write = nfhw


## Simplify failure handling
#BaseFailure = failure.Failure
#class TwistFailure(BaseFailure,BaseException):
#	def __init__(self, exc_value=None, exc_type=None, exc_tb=None):
#		try:
#			a,b,c = sys.exc_info()
#		except Exception:
#			a,b,c = sys.exc_info()
#		if exc_type is None: exc_type = a
#		if exc_value is None: exc_value = b
#		if exc_tb is None: exc_tb = c
#
#		if exc_value is None:
#			raise failure.NoCurrentExceptionError
#		if isinstance(exc_value,BaseFailure):
#			if exc_value.type is not None:
#				exc_type = exc_value.type
#			if exc_value.tb is not None:
#				exc_tb = exc_value.tb
#			if exc_value.value is not None:
#				exc_value = exc_value.value
#		elif not isinstance(exc_value,BaseException):
#			exc_type = RuntimeError("Bad Exception: %s / %s" % (str(exc_value),str(exc_value)))
#		BaseFailure.__init__(self,exc_value,exc_type,exc_tb)
#
#	def cleanFailure(self):
#		"""Do not clean out the damn backtrace. We need it."""
#		pass
#
#failure.Failure = TwistFailure

def cleanFailure(self):
	"""Do not clean out the damn backtrace. We need it."""
	pass

#failure.Failure.cleanFailure = cleanFailure
#
#_tig = failure.Failure.throwExceptionIntoGenerator
#def tig(self,g):
#	if isinstance(self.value,str):
#		self.type = RuntimeError
#		self.value = RuntimeError(self.value)
#	_tig(self,g)
#failure.Failure.throwExceptionIntoGenerator = tig
	
# use NameError because the sqlmix.twisted.DbPool._call() will not retry me
class RemoteError(NameError):
	def __init__(self,type,value):
		self.type = type
		self.value = value
	def __str__(self):
		return "%s: %s <Remote>" % (self.type,self.value)
		
def _tig(self, g):
        """
        This is a clone of #failure.Failure.throwExceptionIntoGenerator()
		except that strings as exception, which may happen with remote calls,
        are converted to a RuntimeError.
        """
        if isinstance(self.value,str):
            self.value = RemoteError(self.type,self.value)
            self.type = RemoteError
        return g.throw(self.type, self.value, self.tb)
failure.Failure.throwExceptionIntoGenerator = _tig

## don't report failures as errors. They _are_ reported at the other end
def _bsf(self, fail, requestID):
	"""Log error and then send it."""
	log.msg("Peer will receive following PB traceback:")
	log.msg(fail)
	self._sendError(fail, requestID)
pb.Broker._sendFailure = _bsf

def _nop(self): pass
failure.Failure.cleanFailure = _nop

if __debug__ and sys.version_info < (2,6,7):
	import threading
	def _note(self, format, *args):
		if getattr(self,'__verbose',False):
			format = format % args
			# Issue #4188: calling current_thread() can incur an infinite
			# recursion if it has to create a DummyThread on the fly.
			ident = threading._get_ident()
			try:
				name = threading._active[ident].name
			except KeyError:
				name = "<OS thread %d>" % ident
			format = "%s: %s\n" % (name, format)
			sys.stderr.write(format)
	threading._Verbose._note = _note	


