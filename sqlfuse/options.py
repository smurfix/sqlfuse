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
	opt("username", dest="username", default=None,
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
	opt("atime", dest="atime", choices=('no','mtime','yes'), default=None,
		help="file atime options")
	opt("diratime", dest="diratime", choices=('no','read','access'), default=None,
		help="directory atime options")

	if mode == "mode":
		A = p.add_subparsers(title='subcommands', help='main command modes')
		Ainfo = A.add_parser("info",help="basic node information")
		Ainfo.set_defaults(mode="info")
		Ainfo.add_argument("node", nargs='?', default="default",
			help="file storage node")

		Alist = A.add_parser("list",help="list metadata")
		Alist.set_defaults(mode="list")

		Aadd = A.add_parser("add",help="add new metadata")
		Aadd.set_defaults(mode="add")

		Adel = A.add_parser("del",help="delete some metadata")
		Adel.set_defaults(mode="del")

		Aupdate = A.add_parser("update",help="update some metadata")
		Aupdate.set_defaults(mode="update")


		Badd = Aadd.add_subparsers(title='types', help='data type')

		Baddnode = Badd.add_parser("node",help="directory access point")
		Baddnode.set_defaults(mode2="node")
		Baddnode.add_argument("name", help="Node name")
		Baddnode.add_argument("root", help="use this root")
		Baddnode.add_argument("files", help="Path to storage space")
		Baddnode.add_argument("access", help="Secret for management login")
		Bapg = Baddnode.add_mutually_exclusive_group()
		Bapg.add_argument('--copy','--nocache', action='store_false', dest="cache", help="always copy files")
		Bapg.add_argument('--nocopy','--cache', action='store_true', dest="cache", help="copy files on demand")

		Baddroot = Badd.add_parser("root",help="hierarchy root")
		Baddroot.set_defaults(mode2="root")
		Baddroot.add_argument("name", help="Root name")

		Baddupdater = Badd.add_parser("updater",help="file copying method")
		Baddupdater.set_defaults(mode2="updater")
		Baddupdater.add_argument("src", help="source node")
		Baddupdater.add_argument("dest", help="destination node")
		Baddupdater.add_argument("distance", help="number measuring the copy's efficiency; smaller=better")

		Baum = Baddupdater.add_subparsers(title='methods', help='data copying methods')
		BaumNative = Baum.add_parser("native",help="sqlfuse-to-sqlfuse file transfer")
		BaumNative.set_defaults(method="native")
		BaumNative.add_argument("address", help="IP address to use to reach the source")
		BaumNative.add_argument("tcpport", nargs='?', type=int, help="port to use, to connect to the source")


		Bdel = Adel.add_subparsers(title='types', help='data type')
		
		Bdelnode = Bdel.add_parser("node",help="directory access point")
		Bdelnode.set_defaults(mode2="node")
		Bdelnode.add_argument("name", help="Node name")

		Bdelroot = Bdel.add_parser("root",help="hierarchy root")
		Bdelroot.set_defaults(mode2="root")
		Bdelroot.add_argument("name", help="Root name")

		Bdelupdater = Bdel.add_parser("updater",help="file copying method")
		Bdelupdater.set_defaults(mode2="updater")
		Bdelupdater.add_argument("id", type=int, help="updater ID")


		Bupdate = Aupdate.add_subparsers(title='types', help='data type')

		Bupdatenode = Bupdate.add_parser("node",help="directory access point")
		Bupdatenode.set_defaults(mode2="node")
		Bupdatenode.add_argument("name", help="Node name")
		Bupdatenode.add_argument("--name", dest="newname", help="New name")
		Bupdatenode.add_argument("--storage", dest="files", help="New storage path")
		Bupdatenode.add_argument("--secret", action='store_true', dest="secret", help="Reset the node-specific secret")
		Bupdatenode.add_argument("--access", action='store', help="New management password")
		Buna = Bupdatenode.add_mutually_exclusive_group()
		Buna.add_argument("--address", help="local IP address for SQLfuse connections")
		Buna.add_argument("--anyaddress", action='store_false',dest='address', help="accept SQLfuse connections on all local addresses")
		Bung = Bupdatenode.add_mutually_exclusive_group()
		Bung.add_argument("--tcpport", type=int, help="TCP port to accept SQLfuse connections on")
		Bung.add_argument("--noport", action='store_false',dest='tcpport', help="do not accept SQLfuse connections")
		Bupg = Bupdatenode.add_mutually_exclusive_group()
		Bupg.add_argument('--copy','--nocache', action='store_false', dest="cache", help="always copy files")
		Bupg.add_argument('--nocopy','--cache', action='store_true', dest="cache", help="copy files on demand")

		Bupdateupdater = Bupdate.add_parser("updater",help="file copying method")
		Bupdateupdater.set_defaults(mode2="updater")
		Bupdateupdater.add_argument("id", type=int, help="updater ID")
		Bupdateupdater.add_argument("--distance", dest="distance", help="number measuring the copy's efficiency; smaller=better")

		Bupdateroot = Bupdate.add_parser("root",help="hierarchy root")
		Bupdateroot.set_defaults(mode2="root")
		Bupdateroot.add_argument("name", help="Root name")
		Bupdateroot.add_argument("--name", dest="newname", help="New name")


		Blist = Alist.add_subparsers(title='types', help='data type')

		Blistnode = Blist.add_parser("node",help="directory access point")
		Blistnode.set_defaults(mode2="node")
		Blistnode.add_argument("name",nargs='?', help="Node name (default: list all nodes)")
		Blistnode.add_argument("--root", dest="root", help="limit to this root (default: list all roots' nodes)")

		Blistprogress = Blist.add_parser("progress",help="file copy in progress")
		Blistprogress.set_defaults(mode2="progress")
		Blistprogress.add_argument("id",nargs='?', type=int, help="progress ID (default: list all progress nodes)")
		Blistprogress.add_argument("--src", dest="src", help="limit to copies from this node")
		Blistprogress.add_argument("--dest", dest="dest", help="limit to copies to this node")
		Blistprogress.add_argument("--inode", dest="inode", help="limit to copies of this inode")

		Blistupdater = Blist.add_parser("updater",help="file copying method")
		Blistupdater.set_defaults(mode2="updater")
		Blistupdater.add_argument("id",nargs='?', type=int, help="updater ID (default: list all updaters)")
		Blistupdater.add_argument("--src", dest="src", help="limit to copying from this node")
		Blistupdater.add_argument("--dest", dest="dest", help="limit to copying to this node")
		Blistupdater.add_argument("--method", dest="method", help="limit to copying with this method")

		Blistroot = Blist.add_parser("root",help="hierarchy root")
		Blistroot.set_defaults(mode2="root")
		Blistroot.add_argument("name",nargs='?', help="Root name (default: list all roots)")

		Blistinode = Blist.add_parser("inode",help="one file")
		Blistinode.set_defaults(mode2="inode")
		Blistinode.add_argument("inum", type=int, help="Inode number")

		Blistevent = Blist.add_parser("event",help="one file")
		Blistevent.set_defaults(mode2="event")
		Blistevent.add_argument("event_id", type=int, help="Event number")

		Blistdir = Blist.add_parser("dir",help="list directory")
		Blistdir.set_defaults(mode2="dir")
		Blistdir.add_argument("root", type=int, help="root to search from")
		Blistdir.add_argument("path", type=str, help="file path to walk")

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


