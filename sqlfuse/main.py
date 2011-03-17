# -*- coding: utf-8 -*-

#    Copyright (C) 2010,2011  Matthias Urlichs <matthias@urlichs.de>
#
#    This program may be distributed under the terms of the GNU GPLv3.
#
## This file is formatted with tabs.
## Do NOT introduce leading spaces.

from __future__ import division, print_function, absolute_import

__all__ = ('SqlFuse',)

"""\
This module implements the main server object. It opens a FUSE port.

Writing is handled directly -- the transport back-end coalesces write requests.
The kernel's FUSE reader cannot handle this.

TODO: It's a protocol. Treat it as such.
"""

import errno, os, stat, sys, traceback
from traceback import print_exc
from twistfuse.filesystem import FileSystem
from twistfuse.kernel import FUSE_ATOMIC_O_TRUNC,FUSE_ASYNC_READ,FUSE_EXPORT_SUPPORT,FUSE_BIG_WRITES
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue, DeferredLock

from sqlfuse import DBVERSION
from sqlfuse.fs import SqlInode,SqlDir,SqlFile, BLOCKSIZE
from sqlfuse.background import RootUpdater,Recorder,NodeCollector,CacheRecorder
from sqlfuse.node import SqlNode,NoLink
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
	changer = DummyChanger()
	db = DummyQuit()
	servers = []

	# 0: no atime; 1: only if <mtime; 2: always
	atime = 1

	# 0: no atime: 1: when reading; 2: also when traversing
	diratime = 0

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

	def handle_exc(*a,**k): #self,fn,exc):
		log_call()
		traceback.print_exc()
		self.db.rollback()

	def done(self, exc=None):
		if exc is None:
			self.db.commit()
		else:
			self.db.rollback()


	### a few FUSE calls which are not handled by the inode object

	@inlineCallbacks
	def rename(self, inode_old, name_old, inode_new, name_new, ctx=None):
		with self.db() as db:
			old_inode = yield inode_old._lookup(name_old,db)
			try:
				yield inode_new._unlink(name_new, ctx=ctx,db=db)
			except EnvironmentError as e:
				if e.errno != errno.ENOENT:
					raise
			yield inode_new._link(old_inode,name_new, ctx=ctx,db=db)
			yield inode_old._unlink(name_old, ctx=ctx,db=db)

		returnValue( None )


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
		with self.db() as db:
			s['blocks'],s['files'] = yield db.DoFn("select nblocks,nfiles from root where id=${root}", root=self.root_id)
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
			raise NoLink(dest)

		if dest == node:
			return getattr(rem,"do_"+name)(*a,**k)
		else:
			return rem.remote_exec(node,name,*a,**k)

	@inlineCallbacks
	def each_node(self,name,*a,**k):
		e1 = None
		if not self.topology:
			print("EACH","no nodes")
			raise RuntimeError("No topology information available")
		#for dest in self.topology.keys():
		for dest in self.neighbors:
			print("EACH",dest,self.topology[dest])
			try:
				d = self.call_node(dest,name,*a,**k)
				def pr(r):
					r.printTraceback(file=sys.stderr)
					return r
				d.addErrback(pr)
				res = yield d
				print("EACH",dest,res)
			except Exception as e:
				print_exc()
				if e1 is None:
					e1 = e
			else:
				returnValue(res)
		raise e1

	@inlineCallbacks
	def init_db(self,db,node):
		"""\
			Setup the database part of the file system's operation.
			"""
		# TODO: setup a copying thread
		self.db = db
		self.node = node
		with self.db() as db:
			try:
				self.node_id,self.root_id,self.inum,self.store,self.port = yield db.DoFn("select node.id,root.id,root.inode,node.files,node.port from node,root where root.id=node.root and node.name=${name}", name=node)
			except NoData:
				raise RuntimeError("data for '%s' is missing"%(self.node,))
			try:
				mode, = yield db.DoFn("select mode from inode where id=${inode}",inode=self.inum)
			except NoData:
				raise RuntimeError("database has not been initialized: inode %d is missing" % (self.inode,))
			if mode == 0:
				db.Do("update inode set mode=${dir} where id=${inode}", dir=stat.S_IFDIR|stat.S_IRWXU|stat.S_IRWXG|stat.S_IRWXO, inode=self.inum)
		
			self.info = Info()
			yield self.info._load(db)
			if self.info.version != DBVERSION:
				raise RuntimeError("Need database version %s, got %s" % (DBVERSION,self.info.version))

			root = SqlInode(self,self.inum)
			yield root._load(db)
		super(SqlFuse,self).__init__(root=root, filetype=SqlFile, dirtype=SqlDir)
		returnValue( None )

	@inlineCallbacks
	def init(self, opt):
		"""\
			Last step before running the file system mainloop.
			"""
		if opt.atime: self.atime = {'no':0,'mtime':1,'yes':2}[opt.atime]
		if opt.diratime: self.diratime = {'no':0,'read':1,'access':2}[opt.diratime]
		self.rooter = RootUpdater(self)
		yield self.rooter.start()
		self.changer = CacheRecorder(self)
		yield self.changer.start()
		self.record = Recorder(self)
		yield self.record.start()
		self.collector = NodeCollector(self)
		yield self.collector.start()
		yield self.connect_all()
	
	@inlineCallbacks
	def connect_all(self):
		from sqlfuse.connect import METHODS
		for m in METHODS:
			try:
				m = __import__("sqlfuse.connect."+m, fromlist=('NodeServerFactory',))
				m = m.NodeServerFactory(self)
				yield m.connect()
			except NoLink:
				print("No link to node",)
			except Exception:
				traceback.print_exc()
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
				traceback.print_exc()
		
	def mount(self,handler,flags):
		"""\
			FUSE callback.
			"""
		self.handler = handler
		return {'flags': FUSE_ATOMIC_O_TRUNC|FUSE_ASYNC_READ|FUSE_EXPORT_SUPPORT|FUSE_BIG_WRITES}

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
		self.rooter.quit()
		self.rooter = None
		self.record.quit()
		self.record = None
		self.collector.quit()
		self.collector = None
		self.changer.quit()
		self.changer = None
		reactor.iterate(delay=0.05)
		#self.db.stop() # TODO: shutdown databases
		#reactor.iterate(delay=0.05)
		self.db.close()
		self.db = None
		reactor.iterate(delay=0.01)


