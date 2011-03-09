# -*- coding: utf-8 -*-

#    Copyright (C) 2010,2011  Matthias Urlichs <matthias@urlichs.de>
#
#    This program may be distributed under the terms of the GNU GPLv3.
#
## This file is formatted with tabs.
## Do NOT introduce leading spaces.

from __future__ import division, print_function, absolute_import

__all__ = ('nowtuple','log_call','flag2mode', 'DBVERSION')

import datetime,errno,inspect,os

DBVERSION = "0.4.1"

class Info(object):
	def _load(self,db):
		def cb(n,v):
			setattr(self,n,v)
		return db.DoSelect("select name,value from `info`", _callback=cb)


try:
	errno.ENOATTR
except AttributeError:
	errno.ENOATTR=61 # TODO: this is Linux

def nowtuple():
	"""Return the current time as (seconds-from-epoch,milliseconds) tuple."""
	now = datetime.datetime.utcnow()
	return (int(now.strftime("%s")),int(now.strftime("%f000")))

def log_call(depth=0):
	"""Debugging aid"""
	c=inspect.currentframe(1+depth)
	print(">>>",c.f_code.co_name,"@",c.f_lineno,repr(c.f_locals))
	traceback.print_stack()
	
def flag2mode(flags):
	"""translate OS flag (O_RDONLY) into access mode ("r")."""
	## Don't use O_APPEND on the underlying file; it may be too big due
	## to previous errors or whatever. The file position in write()
	## is correct.
	#if flags & os.O_APPEND:
	#	mode = "a"
	#else:
	mode = "w"

	f = (flags & (os.O_WRONLY|os.O_RDONLY|os.O_RDWR))
	if f == os.O_RDONLY:
		mode = "r"
	elif f == os.O_RDWR:
		mode += "+"

	return mode


