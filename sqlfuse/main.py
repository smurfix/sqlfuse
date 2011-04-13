# -*- coding: utf-8 -*-

#    Copyright (C) 2010,2011  Matthias Urlichs <matthias@urlichs.de>
#
#    This program may be distributed under the terms of the GNU GPLv3.
#
## This file is formatted with tabs.
## Do NOT introduce leading spaces.

from __future__ import division, absolute_import

__all__ = ('SqlFuse',)

"""\
This module implements the main server object. It opens a FUSE port.

Writing is handled directly -- the transport back-end coalesces write requests.
The kernel's FUSE reader cannot handle this.

TODO: It's a protocol. Treat it as such.
"""

import errno, os, stat, sys
from twistfuse.filesystem import FileSystem
from twistfuse.kernel import FUSE_ATOMIC_O_TRUNC,FUSE_ASYNC_READ,FUSE_EXPORT_SUPPORT,FUSE_BIG_WRITES
from twisted.application.service import MultiService
from twisted.internet import reactor
from twisted.python import log
from twisted.internet.defer import inlineCallbacks, returnValue, DeferredLock

from sqlfuse import DBVERSION,nowtuple, trace,tracers
from sqlfuse.fs import SqlInode,SqlDir,SqlFile, BLOCKSIZE,DB_RETRIES
from sqlfuse.background import RootUpdater,InodeCleaner,Recorder,NodeCollector,InodeWriter,CacheRecorder,UpdateCollector,CopyWorker
from sqlfuse.node import SqlNode,NoLink,MAX_BLOCK
from sqlmix.twisted import DbPool,NoData


class DummyQuit(object):
	def quit(self): pass
	def close(self): pass
class DummyRecorder(DummyQuit):
	def delete(self,inode): pass
	def new(self,inode): pass
	def change(self,inode,data): pass
	def finish_write(self,inode): pass
class DummyChanger(DummyQuit):
	def note(self,inode): pass
class DummyRooter(DummyQuit):
	def d_inode(self,delta): pass
	def d_dir(self,delta): pass
	def d_size(self,old,new): pass

class Info(object):
	def _load(self,db):
		def cb(n,v):
			setattr(self,n,v)
		return db.DoSelect("select name,value from `info`", _callback=cb)

class RemoteDict(dict):
	"""\
		This dictionary stores references to all known remote nodes.

		Nodes are instantiated simply by looking them up.
		Note that actually connecting to them is a different problem.
		"""
	def __init__(self,filesystem):
		self.filesystem = filesystem
	def __getitem__(self, id):
		try:
			return dict.__getitem__(self,id)
		except KeyError:
			r = SqlNode(self.filesystem,id)
			dict.__setitem__(self,id,r)
			return r
	def __setitem__(self, id, r):
		raise KeyError("You cannot set any values this way")
	def __delitem__(self, id):
		try:
			r = dict.__getitem__(self,id)
		except KeyError:
			pass
		else:
			dict.__delitem__(self,id)
			r.disconnect()


class SqlFuse(FileSystem):
	MOUNT_OPTIONS={'allow_other':None, 'suid':None, 'dev':None, 'exec':None, 'fsname':'fuse.sql'}

	rooter = DummyRooter()
	record = DummyRecorder()
	collector = DummyQuit()
	cleaner = DummyQuit()
	changer = DummyChanger()
	ichanger = DummyChanger()
	updatefinder = DummyQuit()
	copier = DummyQuit()

	db = DummyQuit()
	servers = []

	# 0: no atime; 1: only if <mtime; 2: always
	atime = 1

	# 0: no atime: 1: when reading; 2: also when traversing
	diratime = 0
	slow = False

	shutting_down = False

	topology = None  # .topology wants to be an OrderedDict,
	neighbors = None # but that's py2.7 and I'm too lazy to backport that.

	def __init__(self,*a,**k):
		self._slot = {}
		self._slot_next = 1
		self._busy = {}
		self._update = {}
		self._xattr_name = {}
		self._xattr_id = {}
		self._xattr_lock = DeferredLock() # protects name/id translation

		self.FileType = SqlFile
		self.DirType = SqlDir
		self.ENTRY_VALID = (10,0)
		self.ATTR_VALID = (10,0)

		self.remote = RemoteDict(self)
		# Note: Calling super().__init__ will happen later, in init_db()
	

# map fdnum ⇒ filehandle
	def new_slot(self,x):
		"""\
			Remember a file/dir handler. Return an ID.
			"""
		self._slot_next += 1
		while self._slot_next in self._slot:
			if self._slot_next == 999999999:
				self._slot_next = 1
			else:
				self._slot_next += 1
		self._slot[self._slot_next] = x
		return self._slot_next
	def old_slot(self,x):
		"""\
			Fetch a file/dir handler, given its ID.
			"""
		return self._slot[x]
	def del_slot(self,x):
		"""\
			Fetch a file/dir handler, given its ID.

			As this will be the last access, also delete the mapping.
			"""
		res = self._slot[x]
		del self._slot[x]
		return res

#	def _inode_path(self, path, tail=0):
#		path = path.split('/')
#		while path:
#			name = path.pop()
#			if name != '':
#				break
#		if not tail:
#			path.append(name)
#		depth=0
#		q=[""]
#		qa = {"root":self.inode}
#		for p in path:
#			if p == '':
#				continue
#			depth += 1
#			q.append("JOIN tree AS t%d ON t%d.inode = t%d.parent and t%d.name=${t%d_name}" % (depth, depth-1, depth, depth, depth))
#			qa["t"+str(depth)+"_name"] = p
#		q[0]="SELECT t%d.inode from tree as t0" % (depth,)
#		q.append("where t0.inode=${root}")
#		ino, = self.db.DoFn(" ".join(q),**qa)
#		return ino,name

	### a few FUSE calls which are not handled by the inode object

	def rename(self, inode_old, name_old, inode_new, name_new, ctx=None):
		# This is atomic, as it's a transaction
		@inlineCallbacks
		def do_rename(db):
			old_inode = yield inode_old._lookup(name_old,db)
			try:
				yield inode_new._unlink(name_new, ctx=ctx,db=db)
			except EnvironmentError as e:
				if e.errno != errno.ENOENT:
					raise
			yield db.Do("update tree set name=${nname},parent=${ninode} where name=${oname} and parent=${oinode}", nname=name_new, ninode=inode_new.nodeid, oname=name_old, oinode=inode_old.nodeid)
			def adj_size():
				old_inode.mtime = nowtuple()
				old_inode.size -= len(name_old)+1
				new_inode.mtime = nowtuple()
				new_inode.size += len(name_new)+1
			db.call_committed(adj_size)
			returnValue( None )
		return self.db(do_rename, DB_RETRIES)


## not supported, we're not file-backed
#	def bmap(self, *a,**k):
#		log_call()
#		raise IOError(errno.EOPNOTSUPP)

## not used, because the 'default_permissions' option is set
#	def access(self, inode, mode, ctx):
#		log_call()
#		raise IOError(errno.EOPNOTSUPP)


	@inlineCallbacks
	def statfs(self):
		"""\
		File system status.
		We recycle some values, esp. free space, from the underlying storage.
		"""
		s = {}
		osb = os.statvfs(self.store)
		s['bsize'] = BLOCKSIZE
		s['frsize'] = BLOCKSIZE
		s['blocks'],s['files'] = yield self.db(lambda db: db.DoFn("select nblocks,nfiles from root where id=${root}", root=self.root_id), DB_RETRIES)
		s['bfree'] = (osb.f_bfree * osb.f_bsize) // BLOCKSIZE
		s['bavail'] = (osb.f_bavail * osb.f_bsize) // BLOCKSIZE
		s['ffree'] = osb.f_ffree
		# s['favail'] = osb.f_favail
		s['namelen'] = int(self.info.namelen) # see SQL schema

		s['blocks'] += s['bfree']
		s['files'] += s['ffree']
		returnValue( s )


	## xattr back-end. The table uses IDs because they're much shorter than the names.
	## This code only handles the name/ID caching; actual attribute access is in the inode.

	@inlineCallbacks
	def xattr_name(self,xid,db):
		"""\
			xattr key-to-name translation.

			Data consistency states that there must be one.
			"""
		try: returnValue( self._xattr_name[xid] )
		except KeyError: pass

		yield self._xattr_lock.acquire()
		try:
			try: returnValue( self._xattr_name[xid] )
			except KeyError: pass

			name, = yield db.DoFn("select name from xattr_name where id=${xid}",xid=xid)

			self._xattr_name[xid] = name
			self._xattr_id[name] = xid
			def _drop():
				del self._xattr_name[xid]
				del self._xattr_id[name]
			db.call_rolledback(_drop)
		finally:
			self._xattr_lock.release()

		returnValue( name )

	@inlineCallbacks
	def xattr_id(self,name,db,add=False):
		"""\
			xattr name-to-key translation.

			Remembers null mappings, or creates a new one if @add is set.
			"""
		if len(name) == 0 or len(name) > self.info.attrnamelen:
			raise IOError(errno.ENAMETOOLONG)
		try: returnValue( self._xattr_id[name] )
		except KeyError: pass

		try:
			yield self._xattr_lock.acquire()

			try: returnValue( self._xattr_id[name] )
			except KeyError: pass

			try:
				xid, = yield db.DoFn("select id from xattr_name where name=${name}", name=name)
			except NoData:
				if not add:
					self._xattr_id[name] = None
					returnValue( None )
				xid = yield db.Do("insert into xattr_name(name) values(${name})", name=name)

			self._xattr_name[xid] = name
			self._xattr_id[name] = xid
			def _drop():
				del self._xattr_name[xid]
				del self._xattr_id[name]
			db.call_rolledback(_drop)
		finally:
			self._xattr_lock.release()

		returnValue( xid )

	def call_node(self,dest,name,*a,**k):
		try:
			node = self.topology[dest]
			rem = self.remote[node]
		except KeyError:
			trace('error',"NoLink! %s %s %s %s",dest,name,repr(a),repr(k))
			raise NoLink(dest)

		if dest == node:
			return getattr(rem,"do_"+name)(*a,**k)
		else:
			return rem.remote_exec(node,name,*a,**k)

	@inlineCallbacks
	def each_node(self,chk,name,*a,**k):
		e = None
		if not self.topology:
			raise RuntimeError("No topology information available")
		#for dest in self.topology.keys():
		for dest in self.neighbors:
			try:
				d = self.call_node(dest,name,*a,**k)
				def pr(r):
					log.err(r,"EachNode %d: %s" % (dest,name))
					return r
				d.addErrback(pr)
				res = yield d
			except Exception:
				if e is None:
					e = sys.exc_info()
			else:
				if chk and chk():
					returnValue(res)
		if e is None:
			raise NoLink("any node")
		raise e[0],e[1],e[2]

	@inlineCallbacks
	def init_db(self,db,node):
		"""\
			Setup the database part of the file system's operation.
			"""
		# TODO: setup a copying thread
		self.db = db
		reactor.addSystemEventTrigger('during', 'shutdown', db.stopService)

		self.node = node

		@inlineCallbacks
		def do_init_db(db):
			try:
				self.node_id,self.root_id,self.inum,self.store,self.port = yield db.DoFn("select node.id,root.id,root.inode,node.files,node.port from node,root where root.id=node.root and node.name=${name}", name=node)
			except NoData:
				raise RuntimeError("data for '%s' is missing"%(self.node,))

			nnodes, = yield db.DoFn("select count(*) from node where root=${root} and id != ${node}", root=self.root_id, node=self.node_id)
			self.single_node = not nnodes

			try:
				mode, = yield db.DoFn("select mode from inode where id=${inode}",inode=self.inum)
			except NoData:
				raise RuntimeError("database has not been initialized: inode %d is missing" % (self.inode,))
			if mode == 0:
				yield db.Do("update inode set mode=${dir} where id=${inode}", dir=stat.S_IFDIR|stat.S_IRWXU|stat.S_IRWXG|stat.S_IRWXO, inode=self.inum)
		
			self.info = Info()
			yield self.info._load(db)
			if self.info.version != DBVERSION:
				raise RuntimeError("Need database version %s, got %s" % (DBVERSION,self.info.version))

			root = SqlInode(self,self.inum)
			yield root._load(db)
			returnValue( root )

		root = yield self.db(do_init_db)
		super(SqlFuse,self).__init__(root=root, filetype=SqlFile, dirtype=SqlDir)

	@inlineCallbacks
	def init(self, opt):
		"""\
			Last step before running the file system mainloop.
			"""
		if opt.atime: self.atime = {'no':0,'mtime':1,'yes':2}[opt.atime]
		if opt.diratime: self.diratime = {'no':0,'read':1,'access':2}[opt.diratime]
		if opt.slow: self.slow = True
		self.services = MultiService()
		for a,b in (('rooter',RootUpdater),
				('updatefinder',UpdateCollector), ('changer',CacheRecorder),
				('record',Recorder), ('collector', NodeCollector), ('ichanger',InodeWriter),
				('copier',CopyWorker), ('cleaner', InodeCleaner)):
			b = b(self)
			setattr(self,a,b)
			b.setServiceParent(self.services)
		reactor.addSystemEventTrigger('before', 'shutdown', self.services.stopService)
		yield self.services.startService()
		yield self.connect_all()
		self.record.trigger()
	
	@inlineCallbacks
	def connect_all(self):
		from sqlfuse.connect import METHODS
		for m in METHODS:
			try:
				m = __import__("sqlfuse.connect."+m, fromlist=('NodeServerFactory',))
				m = m.NodeServerFactory(self)
				yield m.connect()
			except NoLink:
				log.err("No link to nodes %s"%(m,))
			except Exception:
				f = failure.Failure()
				log.err(f,"No link to nodes %s"%(m,))
			else:
				self.servers.append(m)
		pass

	def disconnect_all(self):
		srv = self.servers
		self.servers = []
		for s in srv:
			try:
				yield s.disconnect()
			except Exception:
				log.err(None,"Disconnect")
		
	def mount(self,handler,flags):
		"""\
			FUSE callback.
			"""
		self.handler = handler
		return {'flags': FUSE_ATOMIC_O_TRUNC|FUSE_ASYNC_READ|FUSE_EXPORT_SUPPORT|FUSE_BIG_WRITES,
			'max_write':MAX_BLOCK}

	@inlineCallbacks
	def destroy(self):
		"""\
			Unmounting: tell the background job to stop.
			"""
		if self.shutting_down:
			return
		self.shutting_down = True

		self.disconnect_all()
		for k in self.remote.keys():
			del self.remote[k]
		# run all delayed jobs now
		for c in reactor.getDelayedCalls():
			c.reset(0)
		yield self.services.stopService()
		reactor.iterate(delay=0.05)
		#self.db.stop() # TODO: shutdown databases
		#reactor.iterate(delay=0.05)
		self.db.close()
		self.db = None
		reactor.iterate(delay=0.01)


