# -*- coding: utf-8 -*-

#    Copyright (C) 2010,2011  Matthias Urlichs <matthias@urlichs.de>
#
#    This program may be distributed under the terms of the GNU GPLv3.
#
## This file is formatted with tabs.
## Do NOT introduce leading spaces.

from __future__ import division, absolute_import

__all__ = ("SqlInode","flush_inodes","build_path")

BLOCKSIZE = 4096
DB_RETRIES = 5

import errno, fcntl, os, stat, sys
from collections import deque
from threading import Lock
from time import time
from weakref import WeakValueDictionary

from twistfuse.filesystem import Inode,File,Dir
from twistfuse.kernel import XATTR_CREATE,XATTR_REPLACE
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue, DeferredLock, Deferred, succeed, DeferredSemaphore
from twisted.internet.threads import deferToThread, blockingCallFromThread
from twisted.python import log
from twisted.spread import pb

from sqlfuse import nowtuple,log_call,flag2mode, trace,tracer_info,tracers, NoLink
from sqlfuse.range import Range

from sqlmix.twisted import NoData

inode_attrs = frozenset("size mode uid gid atime mtime ctime rdev".split())
inode_xattrs = inode_attrs.union(frozenset("target event".split()))

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
tracer_info['conflict']="inode update conflict (enabled)"
tracers.add('conflict')

# TODO: forcibly close an old file if we reach this limit
# (This requires an ordered hash, i.e. either Py2.7 or reinventing the wheel)
nFiles = DeferredSemaphore(900)

def build_path(store,inode, create=True):
	fp = []
	CHARS="ABCDEFGHIJKLMNOPQRSTUVWXYZ"

	ino = inode % 100
	inode //= 100
	while inode:
		if inode <= len(CHARS): # first 'digit' cannot be 'zero'
			fp.insert(0,CHARS[inode-1])
			break
		fp.insert(0,CHARS[inode % len(CHARS)])
		inode //= len(CHARS)
	p = os.path.join(store, *fp)
	if create and not os.path.exists(p):
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
	write_event = None
	in_use = 0
	do_close = False

	@property
	def node(self):
		return SqlInode(self.fs, self.inum)

	inum = None
	def __new__(cls,filesystem,node):
		if not isinstance(node,int):
			node = node.inum
			# we don't keep a link to the inode, to prevent circular references
		self = _Cache.get(node,None)
		if self is None:
			self = object.__new__(cls)
			_Cache[node] = self
		return self

	def __init__(self,filesystem,node):
		if self.inum is not None:
			# got it from cache
			return
		if not isinstance(node,int):
			node = node.inum
		self.fs = filesystem # filesystem
		self.inum = node # inode ID
		self.q = []
		self.known = None # ranges from all nodes
		self.available = None # ranges available on this node
		self.in_progress = Range() # ranges sought from another node
		self.lock = Lock() # protect file read/write

	def __del__(self):
		if self.file_closer:
			self.file_closer.cancel()
			self.file_closer = None
		self._fclose()

	@inlineCallbacks
	def _maybe_close(self):
		assert self.in_use == 0
		if self._last_file < time()-5 or self.node.fs.shutting_down:
			self.file_closer = None
			yield self._close()
		else:
			self.file_closer = reactor.callLater(10,self._maybe_close)

	@inlineCallbacks
	def _close(self):
		if self.in_use > 0:
			self.do_close = True
			return
		if self.file_closer:
			self.file_closer.cancel()
			self.file_closer = None
		if self.file:
			yield reactor.callInThread(self._fclose)
			nFiles.release()

	def _fclose(self):
		with self.lock:
			if not self.file:
				return
			trace('fs',"%d: close file", self.inum)
			self.file.close()
			self.file = None

	def __repr__(self):
		if self.in_progress:
			return "<C %d %s +%s>" % (self.inum,self.available,self.in_progress)
		else:
			return "<C %d %s>" % (self.inum,self.available)
	__str__ = __repr__

	@inlineCallbacks
	def _load(self,db):
		if self.known is not None:
			return
		try:
			self.cache_id,cache = yield db.DoFn("select id,cached from cache where node=${node} and inode=${inum}", node=self.fs.node_id, inum=self.inum)
		except NoData:
			self.known = Range()
			self.available = Range()
			self.cache_id = None
		else:
			self.known = Range(cache)
			self.available = self.known.filter(None)

	@inlineCallbacks
	def _save(self,db):
		if self.inum is None:
			return

		event = self.write_event
		self.write_event = None
		db.call_rolledback(setattr,self,'write_event',event)

		if self.cache_id is None:
			trace('cacherecord',"new for inode %d: ev=%s range=%s",self.inum, event or "-", str(self.known))
			ev1=",event" if event else ""
			ev2=",${event}" if event else ""
			self.cache_id = yield db.Do("insert into cache(cached,inode,node"+ev1+") values (${data},${inode},${node}"+ev2+")", inode=self.inum, node=self.fs.node_id, data=self.known.encode(), event=event)
		else:
			trace('cacherecord',"old for inode %d: ev=%s range=%s",self.inum, event or "-", str(self.known))
			ev=",event=${event}" if event else ""
			yield db.Do("update cache set cached=${data}"+ev+" where id=${cache}", cache=self.cache_id, data=self.known.encode(), event=event, _empty=True)

	def _file_path(self):
		"""\
			Return the path to my backing-store file.
			"""
		return build_path(self.fs.store,self.inum)

	def _read(self,offset,length):
		with self.lock:
			self.file.seek(offset)
			return self.file.read(length)

	def _write(self,offset,data):
		with self.lock:
			self.file.seek(offset)
			self.file.write(data)

	def _flush(self):
		with self.lock:
			if self.file:
				self.file.flush()

	def _trim(self,size):
		with self.lock:
			self.file.truncate(size)

	def _sync(self, flags):
		with self.lock:
			if self.file:
				if flags & 1 and hasattr(os, 'fdatasync'):
					os.fdatasync(self.file.fileno())
				else:
					os.fsync(self.file.fileno())

	@inlineCallbacks
	def have_file(self, reason):
		if not self.file:
			assert self.in_use == 0
			try:
				yield nFiles.acquire()
				res = yield deferToThread(self._have_file,reason)
				if not res:
					nFiles.release() # some other thread was faster
			except:
				nFiles.release()
				raise
		elif self.file_closer:
			assert self.in_use == 0
			self.file_closer.cancel()
			self.file_closer = None
		else:
			assert self.in_use > 0
		self.in_use += 1
		self._last_file = time()

	def timeout_file(self):
		assert self.in_use > 0
		assert self.file is not None
		assert self.file_closer is None
		self.in_use -= 1
		if not self.in_use:
			if self.do_close:
				self.do_close = False
				self.file_closer = reactor.callLater(0,self._maybe_close)
			else:
				self.file_closer = reactor.callLater(10,self._maybe_close)

	def _have_file(self, reason):
		with self.lock:
			if self.file:
				return False
			ipath=self._file_path()
			try:
				self.file = open(ipath,"r+")
				trace('fs',"%d: open file %s for %s", self.inum,ipath,reason)
			except EnvironmentError as e:
				if e.errno != errno.ENOENT:
					raise
				self.file = open(ipath,"w+")
				trace('fs',"%d: open file %s for %s (new)", self.inum,ipath,reason)
			return True

	@inlineCallbacks
	def remote_data(self, offset, data):
		"""\
			Data arrives.
			"""
		trace('cache',"%s: recv %d @ %d: known %s", self.inum,len(data),offset, self.known)
		if not self.inum:
			return
		if self.file_closer:
			self.file_closer.reset(5)
		yield self.write(offset,data)
		yield self.has(offset,offset+len(data))
		trace('cache',"%s: done; known %s",self.inum,self.known)

	@inlineCallbacks
	def trim(self,end, do_file=True):
		r = Range()
		if end > 0:
			r.add(0,end)
		trace('fs' if do_file else 'cache',"%s: trim to %d",self.inum,end)
		self.known &= r
		self.available &= r
		self.fs.changer.note(self)
		if do_file:
			yield self.have_file("trim")
			try:
				yield deferToThread(self._trim,0)
			finally:
				self.timeout_file()

	@inlineCallbacks
	def write(self,offset,data):
		# This code is called bote locally and by remote_data(),
		# so don't check fs.readonly here!
		yield self.have_file("write")
		try:
			yield deferToThread(self._write,offset,data)
		finally:
			self.timeout_file()

	@inlineCallbacks
	def read(self, offset,length):
		yield self.have_file("read")
		try:
			res = yield deferToThread(self._read,offset,length)
			lr = len(res)
			if lr < length:
				trace('error',"%d: read %d @ %d: only got %d", self.inum, length,offset,lr)
				self.available.delete(offset+lr,offset+length)
				self.known.delete(offset+lr,offset+length)
				# TODO: it's not necessarily gone, we just don't know where.
				# Thus, replace with "unknown" instead. Unfortunately we don't have that. Yet.
				self.fs.changer.note(self)
		finally:
			self.timeout_file()
		returnValue( res )

	def has(self,offset,end):
		"""\
			Note that a data block has arrived.
			"""
		if self.fs.readonly:
			return
		self.in_progress.delete(offset,end)
		self.known.add(offset,end)
		chg = self.available.add(offset,end)

		if chg or self.available.equals(0,self.node.size,None):
			self.fs.changer.note(self) # save in the database
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
		repeat = 0
		r = Range([(offset,offset+length)])
		missing = r - self.available
		while missing:
			if self.fs.readonly:
				returnValue( False )
			unavail = missing - self.known
			if unavail: # requested data unavailable, don't bother
				trace('cache',"%s: unavalable %s", self.inum, unavail)
				returnValue( False )
			todo = missing - self.in_progress
			if todo:
				repeat += 10
				trace('cache',"%s: fetch %s", self.inum, todo)
				# One option: ask each node for 'their' data, then do
				# another pass asking all of them for whatever is missing.
				# However, it's much simpler (and causes less load overall)
				# to just ask every node for everything, in order of proximity.
				notfound = [None]
				def chk(res=None):
					"""Test whether all data are here; if so, don't continue"""
					trace('cache',"%s: Callback %s: check ! %s - %s",self.inum, res, todo,self.available)
					if res is not None:
						if notfound[0] is None:
							notfound[0] = res
						else:
							notfound[0] &= res
					return not (todo - self.available)
				self.in_progress += todo
				try:
					res = yield self.node.fs.each_node(chk,"readfile",self.inum,self,todo)
				except NoLink as e:
					if repeat >= 20 and (r - self.available):
						trace('cache',"%s: no link to %s", self.inum, str(e.node_id) if e.node_id else "?")
						raise
					trace('cache',"%s: No link: %s", self.inum, str(e.node_id) if e.node_id else "?")
				except Exception as e:
					trace('cache',"%s: error: %s", self.inum, e)
					raise
				else:
					if notfound[0] and repeat > 20:
						trace('cache',"%s: Data not found? %s", self.inum,notfound[0])
						returnValue( False )
				finally:
					self.in_progress -= todo

			else:
				repeat += 1
				trace('cache',"%s: wait for %s", self.inum, missing)
				assert missing & self.in_progress
				q = Deferred()
				self.q.append(q)
				yield q

			missing = r - self.available
		trace('cache',"%s: ready", self.inum)
		returnValue( True )
	

# Normally the system caches inode records. However, we may have external
# references (other nodes fetching data), so we guarantee uniqueness here
_Inode = WeakValueDictionary()
_InodeList = deque(maxlen=100)
@inlineCallbacks
def flush_inodes(db):
	for i in _Inode.values():
		yield i._shutdown(db)


class SqlInode(Inode):
	"""\
	This represents an in-memory inode object.
	"""
#	__slots__ = [ \
#		'inum',          # inode number
#		'seq',           # database update tracking
#		'attrs',         # dictionary of current attributes (size etc.)
#		'updated',       # set of attributes not yet written to disk
#		'seq',           # change sequence# on-disk
#		'timestamp',     # time the attributes have been read from disk
#		'tree',          # Operations object
#		'changes',       # Range of bytes written (but not saved in a change record)
#		'cache',         # Range of bytes read from remote nodes
#		'inuse',         # open files on this inode? <0:delete after close
#		'write_timer',   # attribute write timer
#		'no_attrs',      # set of attributes which don't exist
#		]

	@inlineCallbacks
	def _shutdown(self,db):
		"""When shutting down, flush the inode from the system."""
		if self.inum is None or self.seq is None:
			return
		yield self._save(db)
		if self.cache:
			yield self.cache._close()
		try: del self.fs.nodes[self.inum]
		except KeyError: pass

	# ___ FUSE methods ___

	def getattr(self):
		"""Read inode attributes from the database."""
		@inlineCallbacks
		def do_getattr(db):
			res = {'ino':self.inum if self.inum != self.fs.root_inum else 1}
			if stat.S_ISDIR(self.mode): 
				res['nlink'], = yield db.DoFn("select count(*) from tree,inode where tree.parent=${inode} and tree.inode=inode.id and inode.typ='d'",inode=self.inum)
				res['nlink'] += 2 ## . and ..
			else:
				res['nlink'], = yield db.DoFn("select count(*) from tree where inode=${inode}",inode=self.inum)
			for k in inode_attrs:
				res[k] = self[k]
# TODO count subdirectories
#			if stat.S_ISDIR(self.mode): 
#				res['nlink'] += 1
			res['blocks'] = (res['size']+BLOCKSIZE-1)//BLOCKSIZE
			res['blksize'] = BLOCKSIZE

			res = {'attr': res}
			res['nodeid'] = self.inum
			res['generation'] = 1 ## TODO: inodes might be recycled (depends on the database)
			res['attr_valid'] = self.fs.ATTR_VALID
			res['entry_valid'] = self.fs.ENTRY_VALID
			returnValue( res )
		return self.fs.db(do_getattr, DB_RETRIES)

	def setattr(self, **attrs):
		size = attrs.get('size',None)
		if size is not None:
			self.fs.record.trim(self.node)
		if size is not None and self.cache and self.size > size:
			dtrim = self.cache.trim(size)
		else:
			dtrim = None
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
		return dtrim

	@inlineCallbacks
	def open(self, flags, ctx=None):
		"""Existing file."""
		yield self.fs.db(self._load, DB_RETRIES)
		if stat.S_ISDIR(self.mode):
			returnValue( IOError(errno.EISDIR) )
		f = self.fs.FileType(self,flags)
		yield f.open()
		returnValue( f )

	@inlineCallbacks
	def opendir(self, ctx=None):
		"""Existing file."""
		if not stat.S_ISDIR(self.mode):
			returnValue( IOError(errno.ENOTDIR) )
		d = self.fs.DirType(self)
		yield d.open()
		returnValue( d )

	@inlineCallbacks
	def _lookup(self, name, db):
		self.do_atime(is_dir=2)
		if name == ".":
			returnValue( self )
		elif name == "..":
			if self.inum == self.fs.root_inum:
				returnValue( self )
			try:
				inum, = yield db.DoFn("select parent from tree where inode=${inode} limit 1", inode=self.inum)
			except NoData:
				raise IOError(errno.ENOENT, "%d:%s" % (self.inum,name))
		else:
			try:
				inum, = yield db.DoFn("select inode from tree where parent=${inode} and name=${name}", inode=self.inum, name=name)
			except NoData:
				raise IOError(errno.ENOENT, "%d:%s" % (self.inum,name))
		res = SqlInode(self.fs,inum)
		yield res._load(db)
		returnValue( res )
	    
	def lookup(self, name):
		return self.fs.db(lambda db: self._lookup(name,db), 5)
			
	@inlineCallbacks
	def create(self, name, flags,mode, umask, ctx=None):
		"""New file."""
		@inlineCallbacks
		def do_create(db):
			try:
				inum, = yield db.DoFn("select inode from tree where parent=${par} and name=${name}", par=self.inum,name=name)
			except NoData:
				inode = yield self._new_inode(db, name,mode|stat.S_IFREG,ctx)
			else:
				if flags & os.O_EXCL:
					returnValue( IOError(errno.EEXIST) )
				inode = SqlInode(self.fs,inum)
				yield inode._load(db)
	
			res = self.fs.FileType(inode, flags)
			returnValue( (inode,res) )
		inode,res = yield self.fs.db(do_create, DB_RETRIES)

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
		if len(name) == 0 or len(name) > self.fs.info.namelen:
			raise IOError(errno.ENAMETOOLONG)
		now,now_ns = nowtuple()
		if rdev is None: rdev=0 # not NULL
		if target: size=len(target)
		else: size=0
		def adj_size():
			self.mtime = nowtuple()
			self.size += len(name)+1
		db.call_committed(adj_size)

		inum = yield db.Do("insert into inode (root,mode,uid,gid,atime,mtime,ctime,atime_ns,mtime_ns,ctime_ns,rdev,target,size,typ) values(${root},${mode},${uid},${gid},${now},${now},${now},${now_ns},${now_ns},${now_ns},${rdev},${target},${size},${typ})", root=self.fs.root_id,mode=mode, uid=ctx.uid,gid=ctx.gid, now=now,now_ns=now_ns,rdev=rdev,target=target,size=size,typ=mode_char[stat.S_IFMT(mode)])
		yield db.Do("insert into tree (inode,parent,name) values(${inode},${par},${name})", inode=inum,par=self.inum,name=name)
		db.call_committed(self.fs.rooter.d_inode,1)
		
		inode = SqlInode(self.fs,inum)
		yield inode._load(db)
		returnValue( inode )

	@inlineCallbacks
	def forget(self):
		"""\
			Drop this node: save.
			"""
		yield self.fs.db(self._save, DB_RETRIES)
		returnValue (None)
			
	@inlineCallbacks
	def unlink(self, name, ctx=None):
		yield self.fs.db(lambda db: self._unlink(name,ctx=ctx,db=db), DB_RETRIES)
		returnValue( None )

	@inlineCallbacks
	def _unlink(self, name, ctx=None, db=None):
		inode = yield self._lookup(name,db)
		if stat.S_ISDIR(inode.mode):
			returnValue( IOError(errno.EISDIR) )

		yield db.Do("delete from tree where parent=${par} and name=${name}", par=self.inum,name=name)
		cnt, = yield db.DoFn("select count(*) from tree where inode=${inode}", inode=inode.inum)
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
				returnValue( IOError(errno.ENOTDIR) )
			cnt, = yield db.DoFn("select count(*) from tree where parent=${inode}", inode=inode.inum)
			if cnt:
				returnValue( IOError(errno.ENOTEMPTY) )
			db.call_committed(self.fs.rooter.d_dir,-1)
			yield inode._remove(db)
		return self.fs.db(do_rmdir, DB_RETRIES)

	@inlineCallbacks
	def symlink(self, name, target, ctx=None):
		if len(target) > self.fs.info.targetlen:
			returnValue( IOError(errno.EDIR,"Cannot link a directory") )
		inode = yield self.fs.db(lambda db: self._new_inode(db,name,stat.S_IFLNK|(0o755) ,ctx,target=target), DB_RETRIES)
		returnValue( inode )

	def link(self, oldnode,target, ctx=None):
		@inlineCallbacks
		def do_link(db):
			if stat.S_ISDIR(oldnode.mode):
				returnValue( IOError(errno.ENAMETOOLONG,"target entry too long") )
			res = yield self._link(oldnode,target, ctx=ctx,db=db)
			returnValue( res )
		return self.fs.db(do_link, DB_RETRIES)

	@inlineCallbacks
	def _link(self, oldnode,target, ctx=None,db=None):
		try:
			yield db.Do("insert into tree (inode,parent,name) values(${inode},${par},${name})", inode=oldnode.inum,par=self.inum,name=target)
		except Exception:
			returnValue( IOError(errno.EEXIST, "%d:%s" % (self.inum,target)) )
		self.mtime = nowtuple()
		self.size += len(target)+1
		returnValue( oldnode ) # that's what's been linked, i.e. link count +=1
			

	def mknod(self, name, mode, rdev, umask, ctx=None):
		return self.fs.db(lambda db: self._new_inode(db,name,mode,ctx,rdev), DB_RETRIES)

	def mkdir(self, name, mode,umask, ctx=None):
		@inlineCallbacks
		def do_mkdir(db):
			inode = yield self._new_inode(db,name,(mode&0o7777&~umask)|stat.S_IFDIR,ctx)
			db.call_committed(self.fs.rooter.d_dir,1)
			returnValue( inode )
		return self.fs.db(do_mkdir, DB_RETRIES)

	@inlineCallbacks
	def _remove(self,db):
		entries = []
		def app(parent,name):
			entries.append((parent,name))
		yield db.DoSelect("select parent,name from tree where inode=${inode}", inode=self.inum, _empty=True, _callback=app)
		for p in entries:
			p,name = p
			p = SqlInode(self.fs,p)
			yield p._load(db)
			def adj_size(p):
				p.mtime = nowtuple()
				p.size -= len(name)+1
			db.call_committed(adj_size,p)
		yield db.Do("delete from tree where inode=${inode}", inode=self.inum, _empty=True)
		if self.fs.single_node or not stat.S_ISREG(self.mode):
			yield db.Do("delete from inode where id=${inode}", inode=self.inum)
			yield db.call_committed(self.fs.rooter.d_inode,-1)
			if stat.S_ISREG(self.mode):
				yield db.call_committed(self.fs.rooter.d_size,self.size,0)
		else:
			self.fs.record.delete(self)

		yield deferToThread(self._os_unlink)
		del self.fs.nodes[self.inum]
		self.inum = None
		returnValue( None )

	def _os_unlink(self):
		if self.inum is None: return
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
			if self.fs.diratime < is_dir: return
		else:
			if not self.fs.atime: return
			if self.fs.atime == 1 and self.atime > self.mtime: return
		self.atime = nowtuple()

	def _file_path(self):
		return self.cache._file_path()


	def getxattr(self, name, ctx=None):
		@inlineCallbacks
		def do_getxattr(db):
			nid = yield self.fs.xattr_id(name,db,False)
			if nid is None:
				returnValue( IOError(errno.ENOATTR) )
			try:
				val, = yield db.DoFn("select value from xattr where inode=${inode} and name=${name}", inode=self.inum,name=nid)
			except NoData:
				returnValue( IOError(errno.ENOATTR) )
			returnValue( val )

		if name in self.no_attrs:
			return IOError(errno.ENOATTR)
		res = self.fs.db(do_getxattr, DB_RETRIES)
		def noa(r):
			r.trap(IOError)
			if r.value.value != errno.ENOATTR:
				return r
			self.no_attrs.add(name)
			return -errno.ENOATTR
		res.addCallback(noa)
		return res


	def setxattr(self, name, value, flags, ctx=None):
		if name in self.no_attrs:
			self.no_attrs.remove(name)
		if len(value) > self.fs.info.attrlen:
			return IOError(errno.E2BIG)

		@inlineCallbacks
		def do_setxattr(db):
			nid = yield self.fs.xattr_id(name,db,True)
			try:
				yield db.Do("update xattr set value=${value},seq=seq+1 where inode=${inode} and name=${name}", inode=self.inum,name=nid,value=value)
			except NoData:
				if flags & XATTR_REPLACE:
					returnValue( IOError(errno.ENOATTR) )
				yield db.Do("insert into xattr (inode,name,value,seq) values(${inode},${name},${value},1)", inode=self.inum,name=nid,value=value)
			else: 
				if flags & XATTR_CREATE:
					returnValue( IOError(errno.EEXIST) )
			returnValue( None )
		return self.fs.db(do_setxattr, DB_RETRIES)

	def listxattrs(self, ctx=None):
		@inlineCallbacks
		def do_listxattrs(db):
			res = []
			i = yield db.DoSelect("select name from xattr where inode=${inode}", inode=self.inum, _empty=1,_store=1)
			for nid, in i:
				name = yield self.fs.xattr_name(nid,db)
				res.append(name)
			returnValue( res )
		return self.fs.db(do_listxattrs, DB_RETRIES)

	def removexattr(self, name, ctx=None):
		@inlineCallbacks
		def do_removexattr(db):
			nid = self.fs.xattr_id(name, db,False)
			if nid is None:
				returnValue( IOError(errno.ENOATTR) )
			try:
				yield db.Do("delete from xattr where inode=${inode} and name=${name}", inode=self.inum,name=nid)
			except NoData:
				returnValue( IOError(errno.ENOATTR) )
			returnValue( None )
		if name in self.no_attrs:
			return IOError(errno.ENOATTR)
		self.no_attrs.add(name)
		return self.fs.db(do_removexattr, DB_RETRIES)

	def readlink(self, ctx=None):
		self.do_atime()
		return self.target

	# ___ supporting stuff ___

	def __repr__(self):
		if not self.inum:
			return "<SInode>"
		if not self.seq:
			return "<SInode%s %d>" % (typ,self.inum)
		cache = self.cache
		if self.typ == "f":
			typ = ""
		else:
			typ = ":"+self.typ

		if not cache:
			cache = ""
		elif cache.known is None:
			cache = " ??"
		else:
			cache = " "+str(cache.known)
		if not self.updated:
			return "<SInode%s %d:%d%s>" % (typ,self.inum, self.seq, cache)
		return "<SInode%s %d:%d (%s)%s>" % (typ,self.inum, self.seq, " ".join(sorted(self.updated.keys())), cache)
	__str__=__repr__

	def __hash__(self):
		if self.inum:
			return self.inum
		else:
			return id(self)
	
	def __cmp__(self,other):
		if self.inum is None or other.inum is None:
			return id(self)-id(other)
		else:
			return self.inum-other.inum
	def __eq__(self,other):
		if id(self)==id(other):
			return True
		if isinstance(other,SqlInode):
			if self.inum and other and self.inum == other:
				raise RuntimeError("two inodes have the same ID")
		elif self.inum == other:
			return True
		return False

	def __ne__(self,other):
		if id(self)==id(other):
			return False
		if self.inum and other.inum and self.inum == other.inum:
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
#		if self.inum is not None:
#			assert self.inum == inum
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
		self._saving = None
		self._saveq = []
		self.no_attrs = set()
		_InodeList.append(self)
		# defer anything we only need when loaded to after _load is called

	def missing(self, start,end):
		"""\
			The system has determined that some data could not be found
			anywhere.
			"""
		if not self.cache:
			return
		r = Range([(start,end)]) - self.cache.available
		self.cache.known -= r
		self.fs.changer.note(self.cache)

	@inlineCallbacks
	def _load(self, db):
		"""Load attributes from storage"""
		yield self.load_lock.acquire()
		try:
			if not self.inum:
				# probably deleted
				return

			if self.seq:
				yield self._save(db)

			d = yield db.DoFn("select * from inode where id=${inode}", inode=self.inum, _dict=True)
			if self.seq is not None and self.seq == d["seq"]:
				returnValue( None )

			self.attrs = {}
			self.updated = {} # old values

			for k in inode_xattrs:
				if k.endswith("time"):
					v = (d[k],d[k+"_ns"])
				else:
					v = d[k]
				self.attrs[k] = v
			self.seq = d["seq"]
			self.size = d["size"]
			self.timestamp = nowtuple()

			if self.cache is NotKnown:
				if self.typ == mode_char[stat.S_IFREG]:
					self.cache = Cache(self.fs,self)
					yield self.cache._load(db)
				else:
					self.cache = None

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
			if key not in self.updated:
				self.updated[key] = self.attrs[key]
			self.attrs[key] = value
			self.fs.ichanger.note(self)

	# We can't use a DeferredLock here because the "done" callback will run at the end of the transaction,
	# but we might still need to re-enter the thing within the same transaction
	def _save_done(self,db):
		if self._saving != db:
			trace('fs',"%s: save unlock error: %s %s",self,self._saving,db)
			raise RuntimeError("inode _save releasing")
		trace('fs',"%s: save unlock: %s",self,db)
		self._saving = None
		if self._saveq:
			self._saveq.pop(0).callback(None)
	def _up_seq(self,n):
		self.seq = n
	def _up_args(self,updated,d):
		if not self.inum: return
		for k in inode_xattrs:
			if k.endswith("time"):
				v = (d[k],d[k+"_ns"])
			else:
				v = d[k]
			if k in self.updated: # new old value
				self.updated[k] = v
			elif k not in updated: # updated and unchanged value
				self.attrs[k] = v

	@inlineCallbacks
	def _save(self, db, new_seq=None):
		"""Save this inode's attributes"""
		if not self.inum: return
		ch = None

		if not self.updated and not self.changes:
			return
		if self._saving is not None and self._saving != db:
			d = Deferred()
			self._saveq.append(d)
			trace('fs',"%s: save lock wait",self)
			yield d
			trace('fs',"%s: save lock done",self)
			if self._saving is not None:
				raise RuntimeError("inode _save locking")
		if not self.updated and not self.changes:
			return

		if self._saving is None:
			trace('fs',"%s: save locked",self)
			self._saving = db
			do_release = True
		else:
			trace('fs',"%s: save reentered",self)
			assert self._saving is db
			do_release = False
		try:
			args = {}
			for f in self.updated.keys():
				if f.endswith('time'):
					v=self.attrs[f]
					args[f]=v[0]
					args[f+'_ns']=v[1]
				else:
					args[f]=self.attrs[f]
			updated = self.updated
			self.updated = {}

			if self.changes:
				ch = self.changes.encode()
				self.changes = Range()
			else:
				ch = None

			if args:
				try:
					if new_seq:
						raise NoData # don't even try
					yield db.Do("update inode set seq=seq+1, "+(", ".join("%s=${%s}"%(k,k) for k in args.keys()))+" where id=${inode} and seq=${seq}", inode=self.inum, seq=self.seq, **args)
				except NoData:
					try:
						d = yield db.DoFn("select for update * from inode where id=${inode}", inode=self.inum, _dict=True)
					except NoData:
						# deleted inode
						trace('conflict',"inode_deleted %s %s %s",self.inum,self.seq,",".join(sorted(updated.keys())))
						del self.fs.nodes[self.inum]
						self.inum = None
					else:
						if new_seq:
							if new_seq != d['seq']+1:
								raise NoData
						else:
							if seq >= d['seq']:
								raise NoData
							new_seq=d['seq']+1
						trace('fs',"%s: inode_changed seq=%s old=%s new=%s",self,self.seq,d['seq'],new_seq)
						for k,v in updated:
							if k in ('event','size'):
								# always use the latest event / largest file size
								if self[k] < d[k]:
									self[k] = d[k]
									del args[k]
							elif k.endswith('time'):
								# Timestamps only increase, so that's no conflict
								if self[k] < d[k]:
									self[k] = d[k]
									del args[k]
									del args[k+'_ns']
							else:
								if self[k] != d[k] and d[k] != v:
									# three-way difference. Annoy the user.
									trace('conflict',"%d: %s k=%s old=%s loc=%s rem=%s",self.inum,k,v,self[k],d[k])

						if args:
							yield db.Do("update inode set seq=${new_seq}, "+(", ".join("%s=${%s}"%(k,k) for k in args.keys()))+" where id=${inode} and seq=${seq}", inode=self.inum, seq=seq, new_seq=new_seq, **args)
							db.call_committed(self._up_seq,new_seq)
						else:
							db.call_committed(self._up_seq,d["seq"])

						# now update the rest
						db.call_committed(self._up_args,updated,d)
				else:
					db.call_committed(self._up_seq,self.seq+1)

					self.seq += 1
				if "size" in args:
					db.call_committed(self.fs.rooter.d_size,updated["size"],self.attrs["size"])

				def re_upd(self):
					for k,v in updated:
						if k not in self.updated:
							self.updated[k]=v
				db.call_rolledback(re_upd)

			if ch:
				db.call_committed(self.fs.record.change,self,ch)

		finally:
			if do_release:
				db.call_committed(self._save_done,db)
				db.call_rolledback(self._save_done,db)

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
				return self.node.fs.db(self._remove, DB_RETRIES)
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
			self.node.fs.record.new(self.node)
		elif mode[0] == "w":
			retry = True
			mode = "r+"

		if self.mode & os.O_TRUNC:
			yield self.node.cache.trim(0)
		returnValue( None )

	@inlineCallbacks
	def read(self, offset,length, ctx=None, atime=True):
		"""Read file, updating atime"""
		trace('rw',"%d: read %d @ %d, len %d", self.node.inum,length,offset,self.node.size)
		if offset >= self.node.size:
			trace('rw',"%d: return nothing", self.node.inum)
			returnValue( "" )
		if offset+length > self.node.size:
			length = self.node.size-offset
			trace('rw',"%d: return only %d", self.node.inum,length)

		if atime:
			self.node.do_atime()
		cache = self.node.cache
		if cache:
			try:
				res = yield cache.get_data(offset,length)
			except NoLink:
				res = None
			if not res:
				trace('rw',"%d: error (no cached data)", self.node.inum)
				raise NoCachedData(self.node.inum)
			data = yield cache.read(offset,length)
			trace('rw',"%d: return %d (cached)", self.node.inum,len(data))
		else:
			data = yield deferToThread(_read,offset,length)
			trace('rw',"%d: return %d", self.node.inum,len(data))

		returnValue( data )

	@inlineCallbacks
	def write(self, offset,buf, ctx=None):
		"""Write file, updating mtime, possibly updating size"""
		# TODO: use fstat() for size info, access may be concurrent
		if self.node.fs.readonly:
			trace('rw',"%d: write %d @ %d: readonly", self.node.inum,len(buf), offset)
			raise OSError(errno.EROFS)
		trace('rw',"%d: write %d @ %d", self.node.inum,len(buf), offset)

		yield self.node.cache.write(offset,buf)
		end = offset+len(buf)
		if self.node.cache:
			yield self.node.cache.has(offset,end)

		self.node.mtime = nowtuple()
		if self.node.size < end:
			trace('rw',"%d: end now %d", self.node.inum, end)
			self.node.size = end
		self.node.changes.add(offset,end)
		self.writes += 1
		returnValue( len(buf) )

	@inlineCallbacks
	def release(self, ctx=None):
		"""The last process closes the file."""
		yield self.node.fs.db(self.node._save, DB_RETRIES)
		yield self.node.clr_inuse()
		if self.writes:
			self.node.fs.record.finish_write(self.node)
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
		tree = self.node.fs
		self.node.do_atime(is_dir=1)
		db = tree.db

		def _callback(a,b,c,d):
			# need to mangle the type field
			callback(a,mode_type[stat.S_IFMT(b)],c,d)

		# Since we send incremental results here, we can't restart.
		with tree.db() as db:
			if not offset:
				_callback(".",self.node.mode,self.node.inum,1)
			if offset <= 1:
				if self.node.inum == self.node.fs.root_inum:
					_callback("..",self.node.mode,self.node.inum,2)
				else:
					try:
						inum = yield db.DoFn("select '..', inode.mode, inode.id, 2 from tree,inode where tree.inode=${inode} and tree.parent=inode.id limit 1", inode=self.node.inum)
					except NoData:
						pass
					else:
						_callback(*inum)
			yield db.DoSelect("select tree.name, inode.mode, inode.id, inode.id+2 from tree,inode where tree.parent=${par} and tree.inode=inode.id and tree.name != '' and inode.id > ${offset} order by inode", par=self.node.inum,offset=offset-2, _empty=True,_store=True, _callback=_callback)
		returnValue( None )

	@inlineCallbacks
	def release(self, ctx=None):
		yield self.node.clr_inuse()
		returnValue( None )

	def sync(self, ctx=None):
		log_call()
		return

