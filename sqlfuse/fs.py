# -*- coding: utf-8 -*-

#    Copyright (C) 2010,2011  Matthias Urlichs <matthias@urlichs.de>
#
#    This program may be distributed under the terms of the GNU GPLv3.
#
## This file is formatted with tabs.
## Do NOT introduce leading spaces.

from __future__ import division, absolute_import

__all__ = ("SqlInode","flush_inodes")

BLOCKSIZE = 4096
DB_RETRIES = 5

import errno, fcntl, os, stat, sys
from threading import Lock
from time import time
from weakref import WeakValueDictionary

from twistfuse.filesystem import Inode,File,Dir
from twistfuse.kernel import XATTR_CREATE,XATTR_REPLACE
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue, DeferredLock, succeed
from twisted.internet.threads import deferToThread
from twisted.python import log
from twisted.spread import pb

from sqlfuse import nowtuple,log_call,flag2mode, trace,tracer_info

from sqlfuse.range import Range
from sqlmix.twisted import NoData

inode_attrs = frozenset("size mode uid gid atime mtime ctime rdev".split())
inode_xattrs = inode_attrs.union(frozenset("target".split()))

mode_char={}
mode_char[stat.S_IFBLK]  = 'b'
mode_char[stat.S_IFCHR]  = 'c'
mode_char[stat.S_IFDIR]  = 'd'
mode_char[stat.S_IFIFO]  = 'p'
mode_char[stat.S_IFLNK]  = 'l'
mode_char[stat.S_IFREG]  = 'f'
mode_char[stat.S_IFSOCK] = 's'

mode_type={}
mode_type[stat.S_IFBLK]  =  6 # DT_BLK
mode_type[stat.S_IFCHR]  =  2 # DT_CHR
mode_type[stat.S_IFDIR]  =  4 # DT_DIR
mode_type[stat.S_IFIFO]  =  1 # DT_FIFO
mode_type[stat.S_IFLNK]  = 10 # DT_LNK
mode_type[stat.S_IFREG]  =  8 # DT_REG
mode_type[stat.S_IFSOCK] = 12 # DT_SOCK

tracer_info['fs']="file system details"
tracer_info['rw']="read/write-level calls"
tracer_info['cache']="get remote files"

class NotKnown:
	pass
class NoCachedData(BufferError):
	"""\
		The system could not get the data for this file.
		(Stale cache, and no connection to any server which has it)
		"""
	pass

_Cache = WeakValueDictionary()
class Cache(object,pb.Referenceable):
	file_closer = None
	file = None

	@property
	def node(self):
		return SqlInode(self.tree, self.nodeid)

	nodeid = None
	def __new__(cls,tree,node):
		if not isinstance(node,int):
			node = node.nodeid
		self = _Cache.get(node,None)
		if self is None:
			self = object.__new__(cls)
			_Cache[node] = self
		return self

	def __init__(self,tree,node):
		if self.nodeid is not None:
			return
		if not isinstance(node,int):
			node = node.nodeid
		self.tree = tree
		self.nodeid = node
		self.q = []
		self.known = None
		self.available = None
		self.in_progress = Range()
		self.lock = Lock() # protect file read/write

	def __del__(self):
		if self.file_closer:
			self.file_closer.cancel()
			self.file_closer = None
		self._fclose()

	@inlineCallbacks
	def _maybe_close(self):
		if self._last_file < time()-5 or self.node.filesystem.shutting_down:
			self.file_closer = None
			yield self._close()
		else:
			self.file_closer = reactor.callLater(10,self._maybe_close)

	@inlineCallbacks
	def _close(self):
		if self.file_closer:
			self.file_closer.cancel()
			self.file_closer = None
		yield reactor.callInThread(self._fclose)

	def _fclose(self):
		with self.lock:
			if not self.file:
				return
			trace('fs',"%d: close file", self.nodeid)
			self.file.close()
			self.file = None

	def __repr__(self):
		if self.in_progress:
			return "<C %d %s +%s>" % (self.nodeid,self.available,self.in_progress)
		else:
			return "<C %d %s>" % (self.nodeid,self.available)
	__str__ = __repr__

	@inlineCallbacks
	def _load(self,db):
		if self.known is not None:
			return
		try:
			self.cache_id,cache = yield db.DoFn("select id,cached from cache where node=${node} and inode=${inum}", node=self.tree.node_id, inum=self.nodeid)
		except NoData:
			self.known = Range()
			self.available = Range()
			self.cache_id = None
		else:
			self.known = Range(cache)
			self.available = self.known.filter(None)

	def _file_path(self):
		"""\
			Return the path to my backing-store file.
			"""
		fp = []
		CHARS="ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		inode = self.nodeid
		ino = inode % 100
		inode //= 100
		while inode:
			if inode <= len(CHARS): # first 'digit' cannot be 'zero'
				fp.insert(0,CHARS[inode-1])
				break
			fp.insert(0,CHARS[inode % len(CHARS)])
			inode //= len(CHARS)
		p = os.path.join(self.tree.store, *fp)
		if not os.path.exists(p):
			try:
				os.makedirs(p, 0o700)
			except EnvironmentError as e:
				if e.errno != errno.EEXIST:
					raise
			# otherwise we have two threads trying to do the same thing,
			# for different cache objects, which is not a problem
		if ino < 10:
			ino = "0"+str(ino)
		else:
			ino = str(ino)
		return os.path.join(p, ino)

	def _read(self,offset,length):
		with self.lock:
			self._have_file()
			self.file.seek(offset)
			return self.file.read(length)

	def _write(self,offset,data):
		with self.lock:
			self._have_file()
			self.file.seek(offset)
			self.file.write(data)

	def _flush(self):
		with self.lock:
			if self.file:
				self.file.flush()

	def _trim(self,size):
		with self.lock:
			self._have_file()
			self.file.truncate(size)

	def _sync(self, flags):
		with self.lock:
			if self.file:
				if flags & 1 and hasattr(os, 'fdatasync'):
					os.fdatasync(self.file.fileno())
				else:
					os.fsync(self.file.fileno())

	def _have_file(self):
		if not self.file:
			ipath=self._file_path()
			try:
				self.file = open(ipath,"r+")
				trace('fs',"%d: open file %s", self.nodeid,ipath)
			except EnvironmentError as e:
				if e.errno != errno.ENOENT:
					raise
				self.file = open(ipath,"w+")
				trace('fs',"%d: open file %s (new)", self.nodeid,ipath)
			if not self.file_closer:
				self.file_closer = reactor.callLater(15,self._maybe_close)
		self._last_file = time()

	@inlineCallbacks
	def remote_data(self, offset, data):
		"""\
			Data arrives.
			"""
		if self.file_closer:
			self.file_closer.cancel()
			self.file_closer = None
		yield reactor.callInThread(self._write,offset,data)
		yield self.has(offset,offset+len(data))

	def trim(self,end):
		r = Range()
		if end > 0:
			r.add(0,end)
		trace('fs',"%s: trim to %d",self.nodeid,end)
		self.known &= r
		self.available &= r
		self.tree.changer.note(self)
		return deferToThread(self._trim,0)

	def has(self,offset,end):
		"""\
			Note that a data block has arrived.
			"""
		self.in_progress.delete(offset,end)
		self.known.add(offset,end)
		chg = self.available.add(offset,end)

		if chg or self.available.equals(0,self.node.size,None):
			self.tree.changer.note(self) # save in the database
			self._trigger_waiters()

	def _trigger_waiters(self):
		q = self.q
		self.q = []
		for d in q:
			d.callback(None)

	@inlineCallbacks
	def get_data(self, offset,length):
		"""\
			Check if the given data record is available.
			"""
		r = Range([(offset,offset+length)])
		missing = r - self.available
		while missing:
			todo = missing - self.in_progress
			if todo:
				trace('cache',"%s: fetch %s", self.nodeid, todo)
				# One option: ask each node for 'their' data, then do
				# another pass asking all of them for whatever is missing.
				# However, it's much simpler (and causes less load overall)
				# to just ask every node for everything, in order of proximity.
				def chk():
					return not (todo - self.known)
				self.in_progress += todo
				try:
					yield self.node.filesystem.each_node(chk,"readfile",self.nodeid,self,todo)
				finally:
					self.in_progress -= todo

			else:
				trace('cache',"%s: wait for %s", self.nodeid, missing)
				assert missing & self.in_progress
				q = Deferred()
				self.q.append(q)
				yield q

			missing = r - self.available
		returnValue( True )
	

# Normally the system caches inode records. However, we may have external
# references (other nodes fetching data), so we guarantee uniqueness here
_Inode = WeakValueDictionary()
@inlineCallbacks
def flush_inodes(db):
	for i in _Inode.values():
		yield i._shutdown(db)


class SqlInode(Inode):
	"""\
	This represents an in-memory inode object.
	"""
#	__slots__ = [ \
#		'nodeid',        # inode number
#		'seq',           # database update tracking
#		'attrs',         # dictionary of current attributes (size etc.)
#		'old_size',      # inode size, as stored in the database
#		'updated',       # set of attributes not yet written to disk
#		'seq',           # change sequence# on-disk
#		'timestamp',     # time the attributes have been read from disk
#		'tree',          # Operations object
#		'changes',       # Range of bytes written (but not saved in a change record)
#		'cache',         # Range of bytes read from remote nodes
#		'inuse',         # open files on this inode? <0:delete after close
#		'write_timer',   # attribute write timer
#		]

	@inlineCallbacks
	def _shutdown(self,db):
		"""When shutting down, flush the inode from the system."""
		yield self._save(db)
		if self.cache:
			yield self.cache._close()
		del self.filesystem.nodes[self.nodeid]

	# ___ FUSE methods ___

	def getattr(self):
		"""Read inode attributes from the database."""
		@inlineCallbacks
		def do_getattr(db):
			res = {'ino':self.nodeid if self.nodeid != self.filesystem.inum else 1}
			if stat.S_ISDIR(self.mode): 
				res['nlink'], = yield db.DoFn("select count(*) from tree,inode where tree.parent=${inode} and tree.inode=inode.id and inode.typ='d'",inode=self.nodeid)
				res['nlink'] += 2 ## . and ..
			else:
				res['nlink'], = yield db.DoFn("select count(*) from tree where inode=${inode}",inode=self.nodeid)
			for k in inode_attrs:
				res[k] = self[k]
# TODO count subdirectories
#			if stat.S_ISDIR(self.mode): 
#				res['nlink'] += 1
			res['blocks'] = (res['size']+BLOCKSIZE-1)//BLOCKSIZE
			res['blksize'] = BLOCKSIZE

			res = {'attr': res}
			res['nodeid'] = self.nodeid
			res['generation'] = 1 ## TODO: inodes might be recycled (depends on the database)
			res['attr_valid'] = self.filesystem.ATTR_VALID
			res['entry_valid'] = self.filesystem.ENTRY_VALID
			returnValue( res )
		return self.filesystem.db(do_getattr, DB_RETRIES)

	@inlineCallbacks
	def setattr(self, **attrs):
		size = attrs.get('size',None)
		if size is not None:
			yield deferToThread(self.cache._trim,size)
		do_mtime = False; do_ctime = False; did_mtime = False
		for f in inode_attrs:
			if f == "ctime": continue
			v = attrs.get(f,None)
			if v is not None:
				if f == "mode":
					self[f] = stat.S_IFMT(self[f]) | stat.S_IMODE(v)
				else:
					self[f] = v
				if f == "size":
					do_mtime = True
				else:
					do_ctime = True
					if f == "mtime":
						did_mtime = True
		if do_ctime:
			self.ctime = nowtuple()
		if do_mtime and not did_mtime:
			self.mtime = nowtuple()

	@inlineCallbacks
	def open(self, flags, ctx=None):
		"""Existing file."""
		trace('fs',"%s: open file (%s)",self,self.filesystem.FileType)
		yield self.filesystem.db(self._load, DB_RETRIES)
		if stat.S_ISDIR(self.mode):
			raise IOError(errno.EISDIR)
		f = self.filesystem.FileType(self,flags)
		yield f.open()
		trace('fs',"%s: opened file: %s",self,f)
		returnValue( f )

	@inlineCallbacks
	def opendir(self, ctx=None):
		"""Existing file."""
		if not stat.S_ISDIR(self.mode):
			raise IOError(errno.ENOTDIR)
		d = self.filesystem.DirType(self)
		yield d.open()
		returnValue( d )

	@inlineCallbacks
	def _lookup(self, name, db):
		self.do_atime(is_dir=2)
		if name == ".":
			returnValue( self )
		elif name == "..":
			if self.nodeid == self.filesystem.inum:
				returnValue( self )
			try:
				inum, = yield db.DoFn("select parent from tree where inode=${inode} limit 1", inode=self.nodeid)
			except NoData:
				raise IOError(errno.ENOENT, "%d:%s" % (self.nodeid,name))
		else:
			try:
				inum, = yield db.DoFn("select inode from tree where parent=${inode} and name=${name}", inode=self.nodeid, name=name)
			except NoData:
				raise IOError(errno.ENOENT, "%d:%s" % (self.nodeid,name))
		res = SqlInode(self.filesystem,inum)
		yield res._load(db)
		returnValue( res )
	    
	def lookup(self, name):
		return self.filesystem.db(lambda db: self._lookup(name,db), 5)
			
	@inlineCallbacks
	def create(self, name, flags,mode, umask, ctx=None):
		"""New file."""
		@inlineCallbacks
		def do_create(db):
			try:
				inum, = yield db.DoFn("select inode from tree where parent=${par} and name=${name}", par=self.nodeid,name=name)
			except NoData:
				inode = yield self._new_inode(db, name,mode|stat.S_IFREG,ctx)
			else:
				if flags & os.O_EXCL:
					raise IOError(errno.EEXIST)
				inode = SqlInode(self.filesystem,inum)
				yield inode._load(db)
	
			res = self.filesystem.FileType(inode, flags)
			returnValue( (inode,res) )
		inode,res = yield self.filesystem.db(do_create, DB_RETRIES)

		# opens its own database connection and therefore must be outside
		# the sub block, otherwise it'll not see the inner transaction
		yield res.open()
		returnValue( (inode, res) )

	@property
	def typ(self):
		return mode_char[stat.S_IFMT(self.mode)]

	@inlineCallbacks
	def _new_inode(self, db, name,mode,ctx=None,rdev=None,target=None):
		"""\
			Helper to create a new named inode.
			"""
		if len(name) == 0 or len(name) > self.filesystem.info.namelen:
			raise IOError(errno.ENAMETOOLONG)
		now,now_ns = nowtuple()
		if rdev is None: rdev=0 # not NULL
		if target: size=len(target)
		else: size=0
		def adj_size():
			self.mtime = nowtuple()
			self.size += len(name)+1
		db.call_committed(adj_size)

		inum = yield db.Do("insert into inode (root,mode,uid,gid,atime,mtime,ctime,atime_ns,mtime_ns,ctime_ns,rdev,target,size,typ) values(${root},${mode},${uid},${gid},${now},${now},${now},${now_ns},${now_ns},${now_ns},${rdev},${target},${size},${typ})", root=self.filesystem.root_id,mode=mode, uid=ctx.uid,gid=ctx.gid, now=now,now_ns=now_ns,rdev=rdev,target=target,size=size,typ=mode_char[stat.S_IFMT(mode)])
		yield db.Do("insert into tree (inode,parent,name) values(${inode},${par},${name})", inode=inum,par=self.nodeid,name=name)
		db.call_committed(self.filesystem.rooter.d_inode,1)
		
		inode = SqlInode(self.filesystem,inum)
		yield inode._load(db)
		returnValue( inode )

	@inlineCallbacks
	def forget(self):
		"""\
			Drop this node: save.
			"""
		yield self.filesystem.db(self._save, DB_RETRIES)
		returnValue (None)
			
	@inlineCallbacks
	def unlink(self, name, ctx=None):
		yield self.filesystem.db(lambda db: self._unlink(name,ctx=ctx,db=db), DB_RETRIES)
		returnValue( None )

	@inlineCallbacks
	def _unlink(self, name, ctx=None, db=None):
		inode = yield self._lookup(name,db)
		if stat.S_ISDIR(inode.mode):
			raise IOError(errno.EISDIR)

		yield db.Do("delete from tree where parent=${par} and name=${name}", par=self.nodeid,name=name)
		cnt, = yield db.DoFn("select count(*) from tree where inode=${inode}", inode=inode.nodeid)
		if cnt == 0:
			if not inode.defer_delete():
				yield inode._remove(db)
		def adj_size():
			self.mtime = nowtuple()
			if self.size > len(name):
				self.size -= len(name)+1
			else:
				log.err("Size problem, inode %s"%(self,))
				self.size = 0
		db.call_committed(adj_size)
		returnValue( None )

	def rmdir(self, name, ctx=None):
		@inlineCallbacks
		def do_rmdir(db):
			inode = yield self._lookup(name,db)
			if not stat.S_ISDIR(self.mode):
				raise IOError(errno.ENOTDIR)
			cnt, = yield db.DoFn("select count(*) from tree where parent=${inode}", inode=inode.nodeid)
			if cnt:
				raise IOError(errno.ENOTEMPTY)
			db.call_committed(self.filesystem.rooter.d_dir,-1)
			yield inode._remove(db)
		return self.filesystem.db(do_rmdir, DB_RETRIES)

	@inlineCallbacks
	def symlink(self, name, target, ctx=None):
		if len(target) > self.filesystem.info.targetlen:
			raise IOError(errno.EDIR,"Cannot link a directory")
		inode = yield self.filesystem.db(lambda db: self._new_inode(db,name,stat.S_IFLNK|(0o755) ,ctx,target=target), DB_RETRIES)
		returnValue( inode )

	def link(self, oldnode,target, ctx=None):
		@inlineCallbacks
		def do_link(db):
			if stat.S_ISDIR(oldnode.mode):
				raise IOError(errno.ENAMETOOLONG,"target entry too long")
			res = yield self._link(oldnode,target, ctx=ctx,db=db)
			returnValue( res )
		return self.filesystem.db(do_link, DB_RETRIES)

	@inlineCallbacks
	def _link(self, oldnode,target, ctx=None,db=None):
		try:
			yield db.Do("insert into tree (inode,parent,name) values(${inode},${par},${name})", inode=oldnode.nodeid,par=self.nodeid,name=target)
		except Exception:
			raise IOError(errno.EEXIST, "%d:%s" % (self.nodeid,target))
		self.mtime = nowtuple()
		self.size += len(target)+1
		returnValue( oldnode ) # that's what's been linked, i.e. link count +=1
			

	def mknod(self, name, mode, rdev, umask, ctx=None):
		return self.filesystem.db(lambda db: self._new_inode(db,name,mode,ctx,rdev), DB_RETRIES)

	def mkdir(self, name, mode,umask, ctx=None):
		@inlineCallbacks
		def do_mkdir(db):
			inode = yield self._new_inode(db,name,(mode&0o7777&~umask)|stat.S_IFDIR,ctx)
			db.call_committed(self.filesystem.rooter.d_dir,1)
			returnValue( inode )
		return self.filesystem.db(do_mkdir, DB_RETRIES)

	@inlineCallbacks
	def _remove(self,db):
		entries = []
		def app(parent,name):
			entries.append((parent,name))
		yield db.DoSelect("select parent,name from tree where inode=${inode}", inode=self.nodeid, _empty=True, _callback=app)
		for p in entries:
			p,name = p
			p = SqlInode(self.filesystem,p)
			yield p._load(db)
			def adj_size(p):
				p.mtime = nowtuple()
				p.size -= len(name)+1
			db.call_committed(adj_size,p)
		yield db.Do("delete from tree where inode=${inode}", inode=self.nodeid, _empty=True)
		if self.filesystem.single_node or not stat.S_ISREG(self.mode):
			yield db.Do("delete from inode where id=${inode}", inode=self.nodeid)
			yield db.call_committed(self.filesystem.rooter.d_inode,-1)
			if stat.S_ISREG(self.mode):
				yield db.call_committed(self.filesystem.rooter.d_size,self.size,0)
		else:
			self.filesystem.record.delete(self)

		yield deferToThread(self._os_unlink)
		del self.filesystem.nodes[self.nodeid]
		self.nodeid = None
		returnValue( None )

	def _os_unlink(self):
		if self.nodeid is None: return
		if stat.S_ISREG(self.mode):
			try:
				os.unlink(self._file_path())
			except EnvironmentError as e:
				if e.errno != errno.ENOENT:
					raise

	def do_atime(self, is_dir=0):
		"""\
			Rules for atime update.
			"""
		if is_dir:
			if self.filesystem.diratime < is_dir: return
		else:
			if not self.filesystem.atime: return
			if self.filesystem.atime == 1 and self.atime > self.mtime: return
		self.atime = nowtuple()

	def _file_path(self):
		return self.cache._file_path()


	def getxattr(self, name, ctx=None):
		@inlineCallbacks
		def do_getxattr(db):
			nid = yield self.filesystem.xattr_id(name,db,False)
			if nid is None:
				raise IOError(errno.ENOATTR)
			try:
				val, = yield db.DoFn("select value from xattr where inode=${inode} and name=${name}", inode=self.nodeid,name=nid)
			except NoData:
				raise IOError(errno.ENOATTR)
			returnValue( val )
		return self.filesystem.db(do_getxattr, DB_RETRIES)

	def setxattr(self, name, value, flags, ctx=None):
		if len(value) > self.filesystem.info.attrlen:
			raise IOError(errno.E2BIG)

		@inlineCallbacks
		def do_setxattr(db):
			nid = yield self.filesystem.xattr_id(name,db,True)
			try:
				yield db.Do("update xattr set value=${value},seq=seq+1 where inode=${inode} and name=${name}", inode=self.nodeid,name=nid,value=value)
			except NoData:
				if flags & XATTR_REPLACE:
					raise IOError(errno.ENOATTR)
				yield db.Do("insert into xattr (inode,name,value,seq) values(${inode},${name},${value},1)", inode=self.nodeid,name=nid,value=value)
			else: 
				if flags & XATTR_CREATE:
					raise IOError(errno.EEXIST)
			returnValue( None )
		return self.filesystem.db(do_setxattr, DB_RETRIES)

	def listxattrs(self, ctx=None):
		@inlineCallbacks
		def do_listxattrs(db):
			res = []
			i = yield db.DoSelect("select name from xattr where inode=${inode}", inode=self.nodeid, _empty=1,_store=1)
			for nid, in i:
				name = yield self.filesystem.xattr_name(nid,db)
				res.append(name)
			returnValue( res )
		return self.filesystem.db(do_listxattrs, DB_RETRIES)

	def removexattr(self, name, ctx=None):
		@inlineCallbacks
		def do_removexattr(db):
			nid = self.filesystem.xattr_id(name, db,False)
			if nid is None:
				raise IOError(errno.ENOATTR)
			try:
				yield db.Do("delete from xattr where inode=${inode} and name=${name}", inode=self.nodeid,name=nid)
			except NoData:
				raise IOError(errno.ENOATTR)
			returnValue( None )
		return self.filesystem.db(do_removexattr, DB_RETRIES)

	def readlink(self, ctx=None):
		self.do_atime()
		return self.target

	# ___ supporting stuff ___

	def __repr__(self):
		if not self.nodeid:
			return "<SInode>"
		if not self.seq:
			return "<SInode %d>" % (self.nodeid)
		cache = self.cache
		if not cache:
			cache = "-C"
		elif cache.known is None:
			cache = "??"
		else:
			cache = str(cache.known)
		if not self.updated:
			return "<SInode %d:%d %s>" % (self.nodeid, self.seq, cache)
		return "<SInode %d:%d (%s) %s>" % (self.nodeid, self.seq, " ".join(sorted(self.updated)), cache)
	__str__=__repr__

	def __hash__(self):
		if self.nodeid:
			return self.nodeid
		else:
			return id(self)
	
	def __cmp__(self,other):
		if self.nodeid is None or other.nodeid is None:
			return id(self)-id(other)
		else:
			return self.nodeid-other.nodeid
	def __eq__(self,other):
		if id(self)==id(other):
			return True
		if isinstance(other,SqlInode):
			if self.nodeid and other and self.nodeid == other:
				raise RuntimeError("two inodes have the same ID")
		elif self.nodeid == other:
			return True
		return False

	def __ne__(self,other):
		if id(self)==id(other):
			return False
		if self.nodeid and other.nodeid and self.nodeid == other.nodeid:
			raise RuntimeError("two inodes have the same ID")
		return True

	def __new__(cls,filesystem,nodeid):
		self = _Inode.get(nodeid,None)
		if self is None:
			self = object.__new__(cls)
			self.inuse = None
			_Inode[nodeid] = self
		return self
	def __init__(self,filesystem,nodeid):
#		if isinstance(inum,SqlInode): return
#		if self.nodeid is not None:
#			assert self.nodeid == inum
#			return
		if getattr(self,"inuse",None) is not None: return
		super(SqlInode,self).__init__(filesystem,nodeid)
		self.seq = None
		self.attrs = None
		self.timestamp = None
		self.inuse = 0
		self.changes = Range()
		self.cache = NotKnown
		self.load_lock = DeferredLock()
		# defer anything we only need when loaded to after _load is called

	@inlineCallbacks
	def _load(self, db):
		"""Load attributes from storage"""
		yield self.load_lock.acquire()
		try:
			if not self.nodeid:
				# probably deleted
				return

			if self.seq:
				yield self._save(db)

			d = yield db.DoFn("select * from inode where id=${inode}", inode=self.nodeid, _dict=True)
			if self.seq is not None and self.seq == d["seq"]:
				returnValue( None )

			self.attrs = {}
			self.updated = set()

			for k in inode_xattrs:
				if k.endswith("time"):
					v = (d[k],d[k+"_ns"])
				else:
					v = d[k]
				self.attrs[k] = v
			self.seq = d["seq"]
			self.size = d["size"]
			self.old_size = d["size"]
			self.timestamp = nowtuple()

			if self.cache is NotKnown:
				self.cache = None
				if self.typ == 'f':
					self.cache = Cache(self.filesystem,self)
					yield self.cache._load(db)
		finally:
			self.load_lock.release()
		returnValue( None )
	
	def __getitem__(self,key):
		if not self.seq:
			raise RuntimeError("inode data not loaded: "+repr(self))
			# self._load()
		return self.attrs[key]

	def __setitem__(self,key,value):
		if not self.seq:
			raise RuntimeError("inode data not loaded: "+repr(self))
			# self._load()
		if key.endswith("time"):
			assert isinstance(value,tuple)
		else:
			assert not isinstance(value,tuple)
		if self.attrs[key] != value:
			self.attrs[key] = value
			self.updated.add(key)
			self.filesystem.ichanger.note(self)

	@inlineCallbacks
	def _save(self, db, new_seq=None):
		"""Save this inode's attributes"""
		if not self.nodeid: return
		ch = None

		if not self.updated and not self.changes:
			return
		args = {}
		for f in self.updated:
			if f.endswith("time"):
				v=self.attrs[f]
				args[f]=v[0]
				args[f+"_ns"]=v[1]
			else:
				args[f]=self.attrs[f]
		self.updated = set()

		if self.changes:
			ch = self.changes.encode()
			self.changes = Range()
		else:
			ch = None

		if args:
			try:
				if new_seq:
					raise NoData # don't even try
				yield db.Do("update inode set seq=seq+1, "+(", ".join("%s=${%s}"%(k,k) for k in args.keys()))+" where id=${inode} and seq=${seq}", inode=self.nodeid, seq=self.seq, **args)
			except NoData:
				try:
					seq,self.size = yield db.DoFn("select seq,size from inode where id=${inode}", inode=self.nodeid)
				except NoData:
					# deleted inode
					trace('fs',"!!! inode_deleted %s %s %s",self.nodeid,self.seq,self.updated)
					del self.filesystem.nodes[self.nodeid]
					self.nodeid = None
				else:
					# warn
					trace('fs',"!!! inode_changed %s %s %s %s %s",self.nodeid,self.seq,seq,new_seq,repr(args))
					if new_seq:
						assert new_seq == seq+1
					else:
						new_seq=seq+1
					yield db.Do("update inode set seq=${new_seq}, "+(", ".join("%s=${%s}"%(k,k) for k in args.keys()))+" where id=${inode} and seq=${seq}", inode=self.nodeid, seq=seq, new_seq=new_seq, **args)
					self.seq = seq+1

			else:
				self.seq += 1
			if "size" in args and self.size != self.old_size:
				def do_size():
					self.filesystem.rooter.d_size(self.old_size,self.attrs["size"])
					self.old_size = self.size
				db.call_committed(do_size)
		if ch:
			self.filesystem.record.change(self,ch)

		returnValue( None )

# busy-inode flag
	def set_inuse(self):
		"""Increment the inode's busy counter."""
		if self.inuse >= 0:
			self.inuse += 1
		else:
			self.inuse += -1

	def clr_inuse(self):
		"""\
			Decrement the inode's busy counter,
			kill the inode if it reaches zero and the inode is marked for deletion.
			"""
		#return a Deferred if it's to be deleted
		if self.inuse < 0:
			self.inuse += 1
			if self.inuse == 0:
				return self.node.filesystem.db(self._remove, DB_RETRIES)
		elif self.inuse > 0:
			self.inuse -= 1
		else:
			raise RuntimeError("SqlInode %r counter mismatch" % (self,))
		return succeed(False)

	def defer_delete(self):
		"""Mark for deletion if in use."""
		if not self.inuse:
			return False
		if self.inuse > 0:
			self.inuse = -self.inuse
		return True

def _setprop(key):
	def pget(self):
		return self[key]
	def pset(self,val):
		self[key] = val
	setattr(SqlInode,key,property(pget,pset))
for k in inode_xattrs:
	_setprop(k)
del _setprop


class SqlFile(File):
	"""Operations on open files.
	"""
	mtime = None

	def __init__(self, node,mode):
		"""Open the file. Also remember that the inode is in use
		so that delete calls get delayed.
		"""
		super(SqlFile,self).__init__(node,mode)
		self.lock = Lock()
		node.set_inuse()
		self.writes = 0
	
	@inlineCallbacks
	def open(self, ctx=None):
		mode = flag2mode(self.mode)
		retry = False
		if self.mode & os.O_TRUNC:
			self.node.size = 0
			self.node.filesystem.record.new(self.node)
		elif mode[0] == "w":
			retry = True
			mode = "r+"

		if self.mode & os.O_TRUNC:
			yield self.node.cache.trim(0)
		returnValue( None )

	@inlineCallbacks
	def read(self, offset,length, ctx=None):
		"""Read file, updating atime"""
		trace('rw',"%d: read %d @ %d, len %d", self.node.nodeid,length,offset,self.node.size)
		if offset >= self.node.size:
			trace('rw',"%d: return nothing", self.node.nodeid)
			returnValue( "" )
		if offset+length > self.node.size:
			length = self.node.size-offset
			trace('rw',"%d: return only %d", self.node.nodeid,length)

		self.node.do_atime()
		cache = self.node.cache
		if cache:
			res = yield cache.get_data(offset,length)
			if not res:
				trace('rw',"%d: error (no cached data)", self.node.nodeid)
				raise NoCachedData(self.node.nodeid)
			data = yield deferToThread(cache._read,offset,length)
			trace('rw',"%d: return %d (cached)", self.node.nodeid,len(data))
		else:
			data = yield deferToThread(_read,offset,length)
			trace('rw',"%d: return %d", self.node.nodeid,len(data))

		returnValue( data )

	@inlineCallbacks
	def write(self, offset,buf, ctx=None):
		"""Write file, updating mtime, possibly updating size"""
		# TODO: use fstat() for size info, access may be concurrent
		trace('rw',"%d: write %d @ %d", self.node.nodeid,len(buf), offset)

		yield deferToThread(self.node.cache._write,offset,buf)
		end = offset+len(buf)
		if self.node.cache:
			yield self.node.cache.has(offset,end)

		self.node.mtime = nowtuple()
		if self.node.size < end:
			trace('rw',"%d: end now %d", self.node.nodeid, end)
			self.node.size = end
		self.node.changes.add(offset,end)
		self.writes += 1
		returnValue( len(buf) )

	@inlineCallbacks
	def release(self, ctx=None):
		"""The last process closes the file."""
		yield self.node.filesystem.db(self.node._save, DB_RETRIES)
		yield self.node.clr_inuse()
		if self.writes:
			self.node.filesystem.record.finish_write(self.node)
		returnValue( None )

	def flush(self,flags, ctx=None):
		"""One process closed the file"""
		return deferToThread(self.node.cache._flush)

	def sync(self, flags, ctx=None):
		"""\
			self-explanatory :-P

			flags&1: use fdatasync(), if available
		"""
		return self.node.cache._sync(flags)

	def lock(self, cmd, owner, **kw):
		log_call()
		raise IOError(errno.EOPNOTSUPP)
		# The code here is much rather just a demonstration of the locking
		# API than something which actually was seen to be useful.

		# Advisory file locking is pretty messy in Unix, and the Python
		# interface to this doesn't make it better.
		# We can't do fcntl(2)/F_GETLK from Python in a platfrom independent
		# way. The following implementation *might* work under Linux. 
		#
		# if cmd == fcntl.F_GETLK:
		#     import struct
		# 
		#     lockdata = struct.pack('hhQQi', kw['l_type'], os.SEEK_SET,
		#                            kw['l_start'], kw['l_len'], kw['l_pid'])
		#     ld2 = fcntl.fcntl(self.fd, fcntl.F_GETLK, lockdata)
		#     flockfields = ('l_type', 'l_whence', 'l_start', 'l_len', 'l_pid')
		#     uld2 = struct.unpack('hhQQi', ld2)
		#     res = {}
		#     for i in xrange(len(uld2)):
		#          res[flockfields[i]] = uld2[i]
		#  
		#     return fuse.Flock(**res)

		# Convert fcntl-ish lock parameters to Python's weird
		# lockf(3)/flock(2) medley locking API...
#		op = { fcntl.F_UNLCK : fcntl.LOCK_UN,
#			   fcntl.F_RDLCK : fcntl.LOCK_SH,
#			   fcntl.F_WRLCK : fcntl.LOCK_EX }[kw['l_type']]
#		if cmd == fcntl.F_GETLK:
#			raise IOError(errno.EOPNOTSUPP)
#		elif cmd == fcntl.F_SETLK:
#			if op != fcntl.LOCK_UN:
#				op |= fcntl.LOCK_NB
#		elif cmd == fcntl.F_SETLKW:
#			pass
#		else:
#			raise IOError(errno.EINVAL)
#
#		fcntl.lockf(self.fd, op, kw['l_start'], kw['l_len'])


class SqlDir(Dir):
	mtime = None
	def __init__(self, node):
		super(SqlDir,self).__init__(node)
		node.set_inuse()
	
	@inlineCallbacks
	def read(self, callback, offset=0, ctx=None):
		# We use the actual inode as offset, except for . and ..
		# Fudge the value a bit so that there's no cycle.
		tree = self.node.filesystem
		self.node.do_atime(is_dir=1)
		db = tree.db

		def _callback(a,b,c,d):
			# need to mangle the type field
			callback(a,mode_type[stat.S_IFMT(b)],c,d)

		# Since we send incremental results here, we can't restart.
		with tree.db() as db:
			if not offset:
				_callback(".",self.node.mode,self.node.nodeid,1)
			if offset <= 1:
				if self.node.nodeid == self.node.filesystem.inum:
					_callback("..",self.node.mode,self.node.nodeid,2)
				else:
					try:
						inum = yield db.DoFn("select '..', inode.mode, inode.id, 2 from tree,inode where tree.inode=${inode} and tree.parent=inode.id limit 1", inode=self.node.nodeid)
					except NoData:
						pass
					else:
						_callback(*inum)
			yield db.DoSelect("select tree.name, inode.mode, inode.id, inode.id+2 from tree,inode where tree.parent=${par} and tree.inode=inode.id and tree.name != '' and inode.id > ${offset} order by inode", par=self.node.nodeid,offset=offset-2, _empty=True,_store=True, _callback=_callback)
		returnValue( None )

	@inlineCallbacks
	def release(self, ctx=None):
		yield self.node.clr_inuse()
		returnValue( None )

	def sync(self, ctx=None):
		log_call()
		return

