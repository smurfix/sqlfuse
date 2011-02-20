# -*- coding: utf-8 -*-

#    Copyright (C) 2010,2011  Matthias Urlichs <matthias@urlichs.de>
#
#    This program may be distributed under the terms of the GNU GPLv3.
#
## This file is formatted with tabs.
## Do NOT introduce leading spaces.

from __future__ import division, print_function, absolute_import

__all__ = ["options","optsplit"]

import argparse, errno, fcntl, os, stat, sys, traceback
from sqlmix import Db,NoData
import ConfigParser as configparser

class _ns(object):
	k=""
	v=""

def optsplit(txt):
	u"""\
	Split "foo=bar,baz\=quux=what,ever" ⇒ {'foo':'bar','baz=quux':'what,ever'}
	(including quoting)
	"""
	res={}
	s=_ns()
	s.s="k"
	def end_name(eq = False):
		if s.s == "k":
			if eq and s.k:
				s.v = ""
				s.s = "v"
			elif eq:
				raise SyntaxError("empty keyword before '='")
			else:
				res[s.k] = None
				s.k = ""
		elif s.s == "v":
			if not eq and s.v:
				res[s.k] = s.v
				s.k = ""
				s.v = ""
				s.s = "k"
			elif eq:
				raise SyntaxError("'key=foo=val' is ambiguoous")
			else:
				raise SyntaxError("empty value after '='")
		else:
			raise SyntaxError("trailing '\\'")
			
	if not txt:
		return res
	for c in txt:
		if s.s[0] == "x":
			s.s = s.s[1]
		elif c == "," or c == "=":
			end_name(c == "=")
			continue
		elif c == "\\":
			s.s = "x"+s.s
			continue
		if s.s == "k":
			s.k += c
		else:
			s.v += c

	end_name()
	return res


def options(mode=None,args=None):
	"""\
	Generate option list. 'mode' may be
	* "long": use long options ("--database foo")
	* "mode": use long options ("--database foo"), add a 'mode' argument
	* "opt": use sub-options ("-o database=foo"), add 'mountpoint'

	* Config files will be read.
	"""
	has_args = {}

	if args is None:
		args = sys.argv[1:]
	class SubAction(argparse.Action):
		def __call__(self, parser, namespace, values, option_string=None):
			for k,v in optsplit(values[0]).items():
				if not hasattr(namespace,k):
					raise argparse.ArgumentError(None,"Unknown keyword '%s'" % (k,))
				setattr(namespace, k, v)
				has_args[k] = True
	p = argparse.ArgumentParser()
	if mode == "opt":
		p.add_argument('-o', nargs=1, metavar="opt=val[,opt=val...]", action=SubAction)
		def opt(name,**k):
			p.add_argument("-o"+name, **k)
		p.add_argument("mountpoint",
			help="Directory where to mount the file system")

		opt("node", dest="node", default="default",
			help="file storage node name")
	else:
		def opt(name,**k):
			p.add_argument("--"+name, **k)

	opt("cfg", dest="inifile", action="append",
		help="INI-style file with additional configuration")
	opt("user", dest="username", default=None,
		help="SQL user name")
	opt("password", dest="password", default=None,
		help="SQL password")
	opt("host", dest="host", default=None,
		help="SQL database host")
	opt("port", dest="port", default=None, type=int,
		help="SQL database port")
	opt("database", dest="database", default=None,
		help="SQL database name")
	opt("dbtype", dest="dbtype", default=None,
		help="SQL database type")

	if mode == "mode":
		A = p.add_subparsers(title='subcommands', help='main command modes')
		Ainfo = A.add_parser("info",help="basic node information")
		Ainfo.set_defaults(mode="info")
		Ainfo.add_argument("node", nargs='?', default="default",
			help="file storage node")

		Alist = A.add_parser("list",help="list metadata")
		Alist.set_defaults(mode="list")

		Aadd = A.add_parser("add",help="add new metadata")
		Aadd.set_defaults(mode1="add")

		Blist = Alist.add_subparsers(title='types', help='data type')
		Blistnode = Blist.add_parser("node",help="directory access point")
		Blistnode.set_defaults(mode2="node")
		Blistnode.add_argument("name",nargs='?', help="Node name (default: list all nodes)")
		Blistnode.add_argument("--root", help="limit to this root (default: list all roots' nodes)")

		Blistroot = Blist.add_parser("root",help="hierarchy root")
		Blistroot.set_defaults(mode2="root")
		Blistroot.add_argument("name",nargs='?', help="Root name (default: list all roots)")

	res = p.parse_args()
	if res.inifile:
		cfg=configparser.ConfigParser()
		cfg.read(res.inifile)
		for k,v in cfg.items(res.node):
			if hasattr(res,k) and getattr(res,k) is None:
				setattr(res,k,v)
		for k,v in cfg.defaults().items():
			if hasattr(res,k) and getattr(res,k) is None:
				setattr(res,k,v)

	return res


