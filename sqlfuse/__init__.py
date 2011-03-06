# -*- coding: utf-8 -*-

#    Copyright (C) 2010,2011  Matthias Urlichs <matthias@urlichs.de>
#
#    This program may be distributed under the terms of the GNU GPLv3.
#
## This file is formatted with tabs.
## Do NOT introduce leading spaces.

from __future__ import division, print_function, absolute_import

__all__ = ["SqlFuse"]

DBVERSION = "0.2"

BLOCKSIZE = 4096

import errno, fcntl, os, stat, sys, traceback
import inspect
from threading import Lock,Thread,Condition,RLock
from time import sleep,time
from weakref import WeakValueDictionary
from sqlfuse.range import Range
from twistfuse.filesystem import FileSystem,Inode,File,Dir
from twistfuse.kernel import FUSE_ATOMIC_O_TRUNC,FUSE_ASYNC_READ,FUSE_EXPORT_SUPPORT,FUSE_BIG_WRITES
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue, DeferredLock, maybeDeferred
from twisted.internet.threads import deferToThread
from twist import deferToLater
try:
	errno.ENOATTR
except AttributeError:
	errno.ENOATTR=61 # TODO: this is Linux

# For deadlock debugging, I use a modified version of the threading module.
_Lock=Lock
_RLock=RLock
def Lock(name): return _Lock()
def RLock(name): return _RLock()
#import threading;threading._VERBOSE = True

from sqlfuse.heap import Heap
from sqlfuse.options import options
from sqlmix import Db,NoData

inode_attrs = frozenset("size mode uid gid atime mtime ctime rdev".split())
inode_xattrs = inode_attrs.union(frozenset("copies".split()))

import datetime
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

class Info(object):
	def _load(self,db):
		def cb(n,v):
			setattr(self,n,v)
		return db.DoSelect("select name,value from `info`", _callback=cb)


class SqlInode(Inode):
	"""\
	This represents an in-memory inode object.
	"""
#	__slots__ = [ \
#		'inum',          # inode number
#		'seq',           # database update tracking
#		'attrs',         # dictionary of current attributes (size etc.)
#		'old_size',      # inode size, as stored in the database
#		'updated',       # set of attributes not yet written to disk
#		'seq',           # change sequence# on-disk
#		'timestamp',     # time the attributes have been read from disk
#		'tree',          # Operations object
#		#'lock',          # Lock for save vs. update
#		#'writelock',     # Lock for concurrent save
#		'changes',       # Range of bytes written (but not saved in a change record)
#		'inuse',         # open files on this inode? <0:delete after close
#		'write_timer',   # attribute write timer
#		'_delay',        # defer updates (required for creation, which needs to be committed first)
#		]

	# ___ FUSE methods ___

	@inlineCallbacks
	def getattr(self):
		with self.filesystem.db() as db:
			res = {'ino':self.nodeid if self.nodeid != self.filesystem.inum else 1}
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

	def setattr(self, **attrs):
		size = attrs.get('size',None)
		if size is not None:
			with file(self._file_path(),"r+") as f:
				f.truncate(size)
		for f in inode_attrs:
			if f == "atime": continue
			v = attrs.get(f,None)
			if v is not None:
				self[f] = v

	@inlineCallbacks
	def open(self, flags, ctx=None):
		"""Existing file."""
		if stat.S_ISDIR(self.mode):
			raise IOError(errno.EISDIR)
		f = self.filesystem.FileType(self,flags)
		yield f.open()
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
		try:
			inum = yield db.DoFn("select inode from tree where parent=${inode} and name=${name}", inode=self.nodeid, name=name)
			inum, = inum
		except NoData:
			raise IOError(errno.ENOENT, "%d:%s" % (self.nodeid,name))
		res = SqlInode(self.filesystem,inum)
		yield res._load(db)
		returnValue( res )
	    
	@inlineCallbacks
	def lookup(self, name):
		with self.filesystem.db() as db:
			inode = yield self._lookup(name,db)
		returnValue( inode )
			
	@inlineCallbacks
	def create(self, name, flags,mode, umask, ctx=None):
		"""New file."""
		with self.filesystem.db() as db:
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

		# opens its own database connection and therefore must be outside
		# the with block
		yield res.open()
		returnValue(res)

	@inlineCallbacks
	def _new_inode(self, db, name,mode,ctx=None,rdev=None):
		"""\
			Helper to create a new named inode.
			"""
		if len(name) == 0 or len(name) > self.filesystem.info.namelen:
			raise IOError(errno.ENAMETOOLONG)
		now,now_ns = nowtuple()
		if rdev is None: rdev=0
		inum = yield db.Do("insert into inode (mode,uid,gid,atime,mtime,ctime,atime_ns,mtime_ns,ctime_ns,rdev) values(${mode},${uid},${gid},${now},${now},${now},${now_ns},${now_ns},${now_ns},${rdev})", mode=mode, uid=ctx.uid,gid=ctx.gid, now=now,now_ns=now_ns,rdev=rdev)
		yield db.Do("insert into tree (inode,parent,name) values(${inode},${par},${name})", inode=inum,par=self.nodeid,name=name)
		
		inode = SqlInode(self.filesystem,inum)
		yield inode._load(db)
		inode.set_delay(db)
		returnValue( inode )

	@inlineCallbacks
	def forget(self):
		"""\
			Drop this node: save.
			"""
		if self.write_timer:
			reactor.cancelCallLater(self.write_timer)
			self.write_timer = None
		with self.filesystem.db() as db:
			yield self._save(db)
		returnValue (None)
			
	@inlineCallbacks
	def unlink(self, name, ctx=None):
		with self.filesystem.db() as db:
			yield self._unlink(name,ctx=ctx,db=db)
		returnValue( None )

	@inlineCallbacks
	def _unlink(self, name, ctx=None, db=None):
		inode = yield self._lookup(name,db)
		if stat.S_ISDIR(inode["mode"]):
			raise IOError(errno.EISDIR)

		yield db.Do("delete from tree where parent=${par} and name=${name}", par=self.nodeid,name=name)
		cnt, = yield db.DoFn("select count(*) from tree where inode=${inode}", inode=inode.nodeid)
		if cnt == 0:
			if not inode.defer_delete():
				yield inode._remove(db)
		returnValue( None )

	@inlineCallbacks
	def rmdir(self, name, ctx=None):
		with self.filesystem.db() as db:
			inode = yield self._lookup(name,db)
			if not stat.S_ISDIR(mode):
				raise IOError(errno.ENOTDIR)
			cnt, = yield self.db.DoFn("select count(*) from tree where parent=${inode}", inode=inode.nodeid)
			if cnt:
				raise IOError(errno.ENOTEMPTY)
			db.call_committed(self.filesystem.rooter.d_dir,-1)
			yield inode._remove(db)
		returnValue( None )

	def symlink(self, name, target, ctx=None):
		log_call()
		raise IOError(errno.EOPNOTSUPP)

	@inlineCallbacks
	def link(self, oldnode,target, ctx=None):
		with self.filesystem.db() as db:
			if stat.S_ISDIR(oldnode['mode']):
				raise IOError(errno.EISDIR,"Cannot link a directory")
			res = yield self._link(oldnode,target, ctx=ctx,db=db)
		returnValue( res )

	@inlineCallbacks
	def _link(self, oldnode,target, ctx=None,db=None):
		try:
			yield db.Do("insert into tree (inode,parent,name) values(${inode},${par},${name})", inode=oldnode.nodeid,par=self.nodeid,name=target)
		except Exception:
			raise IOError(errno.EEXIST, "%d:%s" % (self.nodeid,target))
		returnValue( oldnode ) # that's what's been linked, i.e. link count +=1
			

	@inlineCallbacks
	def mknod(self, name, mode, rdev, ctx=None):
		log_call()
		with self.filesystem.db() as db:
			inode = yield self._new_inode(db,name,mode,ctx,rdev)
		returnValue( inode )

	@inlineCallbacks
	def mkdir(self, name, mode, ctx=None):
		with self.filesystem.db() as db:
			inode = yield self._new_inode(db,name,(mode&0o7777)|stat.S_IFDIR,ctx)
			db.call_committed(self.filesystem.rooter.d_dir,1)
		returnValue( inode )

	@inlineCallbacks
	def _writer1(self):
		with self.filesystem.db() as db:
			yield self._save(db)
		returnValue( None )
	def _writer(self):
		self.write_timer = None
		self._writer1().addErrback(lambda r: r.printTraceback(file=sys.stderr))

	@inlineCallbacks
	def _remove(self,db):
		try:
			yield deferToThread(os.unlink,self._file_path())
		except EnvironmentError as e:
			if e.errno != errno.ENOENT:
				raise
		if self.write_timer:
			reactor.cancelCallLater(self.write_timer)
			self.write_timer = None

		size, = yield db.DoFn("select size from inode where id=${inode}", inode=self.nodeid)
		yield db.Do("delete from tree where inode=${inode}", inode=self.nodeid, _empty=True)
		self.filesystem.record.delete(self.nodeid)
		#yield db.Do("delete from inode where id=${inode}", inode=self.nodeid)
		yield db.call_committed(self.filesystem.rooter.d_inode,-1)
		yield db.call_committed(self.filesystem.rooter.d_size,size,0)
		self.nodeid = None
		returnValue( None )

	def __delete__(self):
		assert self.write_timer is None
		
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
		p = os.path.join(self.filesystem.store, *fp)
		if not os.path.exists(p):
			os.makedirs(p, 0o700)
		if ino < 10:
			ino = "0"+str(ino)
		else:
			ino = str(ino)
		return os.path.join(p, ino)


	@inlineCallbacks
	def getxattr(self, name, ctx=None):
		with self.filesystem.db() as db:
			nid = yield self.filesystem.xattr_id(name,db)
			try:
				val, = yield db.DoFn("select value from xattr where inode=${inode} and name=${name}", inode=self.nodeid,name=nid)
			except NoData:
				raise IOError(errno.ENOATTR)
		returnValue( val )

	@inlineCallbacks
	def setxattr(self, name, value, ctx=None):
		if len(value) > self.filesystem.info.attrlen:
			raise IOError(errno.E2BIG)
		with self.filesystem.db() as db:
			nid = yield self.filesystem.xattr_id(name,db)
			try:
				yield db.Do("update xattr set value=${value},seq=seq+1 where inode=${inode} and name=${name}", inode=self.nodeid,name=nid,value=value)
			except NoData:
				yield db.Do("insert into xattr (inode,name,value,seq) values(${inode},${name},${value},1)", inode=self.nodeid,name=nid,value=value)
		returnValue( None )

	@inlineCallbacks
	def listxattrs(self, ctx=None):
		res = []
		with self.filesystem.db() as db:
			i = yield db.DoSelect("select name from xattr where inode=${inode}", inode=self.nodeid, _empty=1,_store=1)
			for nid, in i:
				name = yield self.filesystem.xattr_name(nid,db)
				res.append(name)
		returnValue( res )

	@inlineCallbacks
	def removexattr(self, name, ctx=None):
		with self.filesystem.db() as db:
			nid=self.filesystem.xattr_id(name, db)
			try:
				yield db.Do("delete from xattr where inode=${inode} and name=${name}", inode=self.nodeid,name=nid)
			except NoData:
				raise IOError(errno.ENOATTR)
		returnValue( None )


	# ___ supporting stuff ___

	def __repr__(self):
		if not self.nodeid:
			return "<SInode>"
		if not self.seq:
			return "<SInode %d>" % (self.nodeid)
		if not self.updated:
			return "<SInode %d:%d>" % (self.nodeid, self.seq)
		return "<SInode %d:%d (%s)>" % (self.nodeid, self.seq, " ".join(sorted(self.updated)))
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

#	def __new__(cls, inum,tree, is_new=False):
#		""""Return a cached copy, if possible"""
#		if isinstance(inum,cls): return inum
#		try:
#			return tree.inode_cache[inum]
#		except KeyError:
#			inode = object.__new__(cls)
#			tree.inode_cache[inum] = inode
#			inode.nodeid = None
#			return inode
#
	def __init__(self,filesystem,nodeid):
#		if isinstance(inum,SqlInode): return
#		if self.nodeid is not None:
#			assert self.nodeid == inum
#			return
		super(SqlInode,self).__init__(filesystem,nodeid)
		self.seq = None
		self.attrs = None
		self.timestamp = None
		self.inuse = 0
		self.changes = Range()
		self._delay = 0
		self.write_timer = None
		# defer anything we only need when loaded to after _load is called

	@inlineCallbacks
	def _load(self, db):
		"""Load attributes from storage"""
		if not self.nodeid:
			raise RuntimeError("tried to load inode without ID")

		if self.seq:
			## TODO: also check the timestamp, don't load when recent enough
			seq, = yield db.DoFn("select seq from inode where id=${inode}", inode=self.nodeid)
			if self.seq != seq:
				# Oops! somebody else changed the inode
				print("!!! inode_changed",self.nodeid,self.seq,seq,self.updated)
				self.seq = seq
				yield self._save(db)
			else:
				returnValue( None )

		self.attrs = {}
		self.updated = set()
		d = yield db.DoFn("select * from inode where id=${inode}", inode=self.nodeid, _dict=1)
		for k in inode_xattrs:
			if k.endswith("time"):
				v = (d[k],d[k+"_ns"])
			else:
				v = d[k]
			self.attrs[k] = v
		self.seq = d["seq"]
		self.old_size = d["size"]
		self.timestamp = nowtuple()
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
			if self.write_timer is None:
				self.write_timer = reactor.callLater(self.filesystem.ATTR_VALID[0]/2, self._writer)

	@inlineCallbacks
	def _save(self, db):
		"""Save local attributes"""
		if not self.nodeid: return
		if self._delay: return
		ch = None

		# no "with" statement here: after collecting attrs, we need
		# to grab the writelock before releasing the inode lock
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
				yield db.Do("update inode set seq=seq+1, "+(", ".join("%s=${%s}"%(k,k) for k in args.keys()))+" where id=${inode} and seq=${seq}", inode=self.nodeid, seq=self.seq, **args)
			except NoData:
				try:
					seq,self.old_size = yield db.DoFn("select seq,size from inode where id=${inode}", inode=self.nodeid)
				except NoData:
					# deleted inode
					print("!!! inode_deleted",self.nodeid,self.seq,self.updated)
					self.nodeid = None
				else:
					# warn
					print("!!! inode_changed",self.nodeid,self.seq,seq,repr(args))
					yield db.Do("update inode set seq=${seq}+1, "+(", ".join("%s=${%s}"%(k,k) for k in args.keys()))+" where id=${inode} and seq=${seq}", inode=self.nodeid, seq=seq, **args)
					self.seq = seq+1

			else:
				self.seq += 1
			if "size" in self.attrs and self.attrs["size"] != self.old_size:
				def do_size():
					self.filesystem.rooter.d_size(self.old_size,self.attrs["size"])
					self.old_size = self.attrs["size"]
				db.call_committed(do_size)
		if ch:
			self.filesystem.record.change(self,ch)

		returnValue( None )

# busy-inode flag
	def set_inuse(self):
		if self.inuse >= 0:
			self.inuse += 1
		else:
			self.inuse += -1

	def clr_inuse(self):
		#return True if it's to be deleted
		if self.inuse < 0:
			self.inuse += 1
			if self.inuse == 0:
				return True
		elif self.inuse > 0:
			self.inuse -= 1
		else:
			raise RuntimeError("SqlInode %r counter mismatch" % (self,))
		return False

	def defer_delete(self):
		if not self.inuse:
			return False
		if self.inuse > 0:
			self.inuse = -self.inuse
		return True
	def no_defer_delete(self,i):
		if self.inuse < 0:
			self.inuse = -self.inuse

	# background update (necessary after inode creation)
	def set_delay(self,db):
		self._delay = 1
		db.call_committed(self.clr_delay)
		db.call_rolledback(self.del_delay)

	def clr_delay(self):
		self._delay = 0
		self._save()
		
	def del_delay(self):
		self._delay = 0
		self.filesystem.forget(self)
		self.nodeid = None
		
def _setprop(key):
	def pget(self):
		return self[key]
	def pset(self,val):
		self[key] = val
	setattr(SqlInode,key,property(pget,pset))
for k in inode_xattrs:
	_setprop(k)
del _setprop


class BackgroundJob(object):
	"""\
		Do some work in the background. Abstract class; implement .work()
		and possibly change .interval.
		"""
	interval = 1.0
	worker = None
	restart = False

	def __init__(self):
		pass

	def start(self):
		pass

	def trigger(self):
		"""Tell the worker to do something. Sometime later."""
		if self.worker is None:
			self.worker = reactor.callLater(self.interval,self.run)
		else:
			self.restart = True

	def quit(self):
		if self.worker is None:
			return
		self.worker.reset(0)

	def run(self):
		"""Background loop."""

		self.restart = False
		d = maybeDeferred(self.work)
		def done(r):
			self.worker = None
			if self.restart:
				self.trigger()
			return r
		d.addBoth(done)
		d.addErrback(lambda e: e.printTraceback(file=sys.stderr))

	def work(self):
		raise NotImplementedError("You need to override %s.work" % (self.__class__.__name__))

#class BackgroundUpdater(BackgroundJob):
#	def __init__(self,tree):
#		super(BackgroundUpdater,self).__init__()
#		self.tree = tree
#
#	def work(self):
#		"""Sync all inodes"""
#		u = self.tree._update
#		self.tree._update = set()
#		for inode in u:
#			inode._save()
#		self.tree.db.commit()
#		sleep(0.5)
#		return None

class RootUpdater(BackgroundJob):
	def __init__(self,tree):
		super(RootUpdater,self).__init__()
		self.tree = tree
		self.delta_inode = 0
		self.delta_dir = 0
		self.delta_block = 0

	def d_inode(self,delta):
		self.delta_inode += delta
		self.trigger()

	def d_dir(self,delta):
		self.delta_dir += delta
		self.trigger()

	def d_size(self,old,new):
		old = (old+BLOCKSIZE-1)//BLOCKSIZE
		new = (new+BLOCKSIZE-1)//BLOCKSIZE
		if old == new: return
		self.delta_block += new-old
		self.trigger()

	@inlineCallbacks
	def work(self):
		"""Sync root data"""
		d_inode = self.delta_inode; self.delta_inode = 0
		d_dir = self.delta_dir; self.delta_dir = 0
		d_block = self.delta_block; self.delta_block = 0
		if d_inode or d_block or d_dir:
			with self.tree.db() as db:
				yield db.Do("update root set nfiles=nfiles+${inodes}, nblocks=nblocks+${blocks}, ndirs=ndirs+${dirs}", root=self.tree.root_id, inodes=d_inode, blocks=d_block, dirs=d_dir)
		returnValue( None )

class Recorder(BackgroundJob):
	def __init__(self,tree):
		super(Recorder,self).__init__()
		self.tree = tree
		self.data = []

	@inlineCallbacks
	def work(self):
		d = self.data
		self.data = []
		with self.tree.db() as db:
			if not d:
				yield db.Do("insert into `event`(`inode`,`node`,`typ`,`range`) values(${inode},${node},${event},${data})", inode=self.tree.inum,node=self.tree.node_id,data=None,event='s' )
			else:
				for event,inum,data in d:
					yield db.Do("insert into `event`(`inode`,`node`,`typ`,`range`) values(${inode},${node},${event},${data})", inode=inum,node=self.tree.node_id,data=data,event=event )
				self.restart = True
		returnValue( None )
		
	def delete(self,inode):
		self.data.append(('d',inode.nodeid,None))
		self.trigger()
	
	def new(self,inode):
		self.data.append(('n',inode.nodeid,None))
		self.trigger()

	def change(self,inode,data):
		self.data.append(('c',inode.nodeid,data))
		self.trigger()

	def finish_write(self,inode):
		self.data.append(('f',inode.nodeid,None))
		self.trigger()


class SqlFile(File):
	"""Operations on open files.
	"""
	mtime = None
	def __init__(self, node,mode):
		"""Open the file. Also remember that the inode is in use
		so that delete calls get delayed.
		"""
		super(SqlFile,self).__init__(node,mode)
		self.lock = Lock(name="file<%d>"%node.nodeid)
		node.set_inuse()
		self.writes = 0
	
	@inlineCallbacks
	def open(self, ctx=None):
		mode = flag2mode(self.mode)
		ipath=self.node._file_path()
		print("*** OPEN %s %o %o"%(ipath,self.mode,os.O_TRUNC))
		if not os.path.exists(ipath) or self.mode & os.O_TRUNC:
			self.node["copies"] = 1 # ours
			self.node["size"] = 0
			self.node.filesystem.record.new(self.node)
		elif mode[0] == "w":
			mode = "r+"
		self.file = yield deferToThread(open,ipath,mode)
		if self.mode & os.O_TRUNC:
			yield deferToThread(os.ftruncate,self.file.fileno(),0)
		returnValue( None )

	@inlineCallbacks
	def read(self, offset,length, ctx=None):
		"""Read file, updating atime"""
		#self.node.atime = nowtuple()
		def _read():
			with self.lock:
				self.file.seek(offset)
				return self.file.read(length)
		data = yield deferToThread(_read)
		returnValue( data )

	@inlineCallbacks
	def write(self, offset,buf, ctx=None):
		"""Write file, updating mtime, possibly updating size"""
		# TODO: use fstat() for size info, access may be concurrent
		def _write():
			with self.lock:
				self.file.seek(offset)
				self.file.write(buf)
		
		yield deferToThread(_write)
		end = offset+len(buf)
		self.node.mtime = nowtuple()
		if self.node.size < end:
			self.node.size = end
		self.node.changes.add(offset,end)
		self.writes += 1
		returnValue( len(buf) )

	@inlineCallbacks
	def release(self, ctx=None):
		"""The last process closes the file."""
		with self.node.filesystem.db() as db:
			yield self.node._save(db)
			yield deferToThread(self.file.close)
			if self.node.clr_inuse():
				yield self.node.tree._remove(self.node,db)
		if self.writes:
			self.node.filesystem.record.finish_write(self.node)
		returnValue( None )

	def flush(self,flags, ctx=None):
		"""One process closeds the file"""
		return deferToThread(self.file.flush)

	@inlineCallbacks
	def sync(self, flags, ctx=None):
		"""\
			self-explanatory :-P

			flags&1: use fdatasync()
		"""
		if flags & 1 and hasattr(os, 'fdatasync'):
			yield deferToThread(os.fdatasync,self.file.fileno())
		else:
			yield deferToThread(os.fsync,self.file.fileno())
		yield self.node.sync(flags)
		returnValue( None )

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
		db = tree.db
		with tree.db() as db:
			yield db.DoSelect("select tree.name,inode.id,inode.mode,inode.id from tree,inode where tree.parent=${par} and tree.inode=inode.id and tree.name != '' and inode.id > ${offset} order by inode", par=self.node.nodeid,offset=offset, _empty=True,_store=True, _callback=callback)
		returnValue( None )

	@inlineCallbacks
	def release(self, ctx=None):
		if self.node.clr_inuse():
			with self.node.filesystem.db() as db:
				yield self.node._remove(db)
		returnValue( None )

	def sync(self, ctx=None):
		log_call()
		return


class SqlFuse(FileSystem):
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

		# Note: Calling super().__init__ will happen later, in init_db()
	

# map fdnum ⇒ filehandle
	def new_slot(self,x):
		self._slot_next += 1
		while self._slot_next in self._slot:
			if self._slot_next == 999999999:
				self._slot_next = 1
			else:
				self._slot_next += 1
		self._slot[self._slot_next] = x
		return self._slot_next
	def old_slot(self,x):
		return self._slot[x]
	def del_slot(self,x):
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

	def readlink(self, inum, ctx=None):
		log_call()
		raise IOError(errno.EOPNOTSUPP)


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


## xattr. The table uses IDs because they're much shorter than the names.

	@inlineCallbacks
	def xattr_name(self,xid,db):
		"""xattr key-to-name translation"""
		try: returnValue( self._xattr_name[xid] )
		except KeyError: pass

		yield self._xattr_lock.acquire()
		try:
			try: returnValue( self._xattr_name[xid] )
			except KeyError: pass

			# this had better exist ...
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
	def xattr_id(self,name,db):
		"""xattr name-to-key translation"""
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

	def statfs(self):
		"""\
		File system status.
		We recycle some values, esp. free space, from the underlying storage.
		"""
		s = llfuse.StatvfsData()
		osb = os.statvfs(self.store)
		s.f_bsize = BLOCKSIZE
		s.f_frsize = BLOCKSIZE
		s.f_blocks,s.f_files = self.db.DoFn("select nblocks,nfiles from root where id=${root}", root=self.root_id)
		s.f_bfree = (osb.f_bfree * osb.f_bsize) // BLOCKSIZE
		s.f_bavail = (osb.f_bavail * osb.f_bsize) // BLOCKSIZE
		s.f_ffree = osb.f_ffree
		s.f_favail = osb.f_favail
		s.f_namemax = 255 # see SQL schema

		s.f_blocks += s.f_bfree
		s.f_files += s.f_ffree
		return s

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
				self.node_id,self.root_id,self.inum,self.store = yield db.DoFn("select node.id,root.id,root.inode,node.files from node,root where root.id=node.root and node.name=${name}", name=node)
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

	def update_add(self, inode):
		self._update.add(inode)
		self.syncer.trigger()

	def init(self):
		"""\
		Last step before running the file system mainloop.
		"""
		self.rooter = RootUpdater(self)
		self.rooter.start()
		self.record = Recorder(self)
		self.record.start()

	def mount(self,handler,flags):
		self.handler = handler
		return {'flags': FUSE_ATOMIC_O_TRUNC|FUSE_ASYNC_READ|FUSE_EXPORT_SUPPORT|FUSE_BIG_WRITES}

	def destroy(self):
		"""\
		Unmounting: tell the background job to stop.
		"""

		for c in reactor.getDelayedCalls():
			c.reset(0)
		self.rooter.quit()
		self.record.quit()
		reactor.iterate(delay=0.01)
		#self.db.stop() # TODO: shutdown databases
		#reactor.iterate(delay=0.05)
		self.db = None

	def stacktrace(self):
		'''Debugging, triggered when the "fuse_stacktrace" attribute
		is set on the mountpoint.
		'''

		import sys
		
		code = list()
		for threadId, frame in sys._current_frames().items():
			code.append("\n# ThreadID: %s" % threadId)
			for filename, lineno, name, line in traceback.extract_stack(frame):
				code.append('%s:%d, in %s' % (os.path.basename(filename), lineno, name))
				if line:
					code.append("    %s" % (line.strip()))

		print("\n".join(code))

