# -*- coding: utf-8 -*-

#    Copyright (C) 2010,2011  Matthias Urlichs <matthias@urlichs.de>
#
#    This program may be distributed under the terms of the GNU GPLv3.
#
## This file is formatted with tabs.
## Do NOT introduce leading spaces.

from __future__ import division, print_function, absolute_import

__all__ = ("SqlInode",)

BLOCKSIZE = 4096

import errno, fcntl, os, stat, sys
from threading import Lock
from sqlfuse.range import Range
from twistfuse.filesystem import Inode,File,Dir
from twistfuse.kernel import XATTR_CREATE,XATTR_REPLACE
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.threads import deferToThread

from sqlfuse import nowtuple,log_call,flag2mode
from sqlmix.twisted import NoData

inode_attrs = frozenset("size mode uid gid atime mtime ctime rdev".split())
inode_xattrs = inode_attrs.union(frozenset("copies target".split()))

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
		do_mtime = False; do_ctime = False; did_mtime = False
		for f in inode_attrs:
			if f == "ctime": continue
			v = attrs.get(f,None)
			if v is not None:
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
		returnValue( (inode, res) )

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
		self.mtime = nowtuple()
		self.size += len(name)+1

		inum = yield db.Do("insert into inode (root,mode,uid,gid,atime,mtime,ctime,atime_ns,mtime_ns,ctime_ns,rdev,target,size) values(${root},${mode},${uid},${gid},${now},${now},${now},${now_ns},${now_ns},${now_ns},${rdev},${target},${size})", root=self.filesystem.root_id,mode=mode, uid=ctx.uid,gid=ctx.gid, now=now,now_ns=now_ns,rdev=rdev,target=target,size=size)
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
		if self.write_timer:
			self.write_timer.cancel()
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
		if stat.S_ISDIR(inode.mode):
			raise IOError(errno.EISDIR)

		yield db.Do("delete from tree where parent=${par} and name=${name}", par=self.nodeid,name=name)
		cnt, = yield db.DoFn("select count(*) from tree where inode=${inode}", inode=inode.nodeid)
		if cnt == 0:
			if not inode.defer_delete():
				yield inode._remove(db)
		self.mtime = nowtuple()
		self.size -= len(name)+1
		returnValue( None )

	@inlineCallbacks
	def rmdir(self, name, ctx=None):
		with self.filesystem.db() as db:
			inode = yield self._lookup(name,db)
			if not stat.S_ISDIR(self.mode):
				raise IOError(errno.ENOTDIR)
			cnt, = yield db.DoFn("select count(*) from tree where parent=${inode}", inode=inode.nodeid)
			if cnt:
				raise IOError(errno.ENOTEMPTY)
			db.call_committed(self.filesystem.rooter.d_dir,-1)
			yield inode._remove(db)
		returnValue( None )

	@inlineCallbacks
	def symlink(self, name, target, ctx=None):
		if len(target) > self.filesystem.info.targetlen:
			raise IOError(errno.EDIR,"Cannot link a directory")
		with self.filesystem.db() as db:
			inode = yield self._new_inode(db,name,stat.S_IFLNK|(0o755) ,ctx,target=target)
		returnValue( inode )

	@inlineCallbacks
	def link(self, oldnode,target, ctx=None):
		with self.filesystem.db() as db:
			if stat.S_ISDIR(oldnode.mode):
				raise IOError(errno.ENAMETOOLONG,"target entry too long")
			res = yield self._link(oldnode,target, ctx=ctx,db=db)
		returnValue( res )

	@inlineCallbacks
	def _link(self, oldnode,target, ctx=None,db=None):
		try:
			yield db.Do("insert into tree (inode,parent,name) values(${inode},${par},${name})", inode=oldnode.nodeid,par=self.nodeid,name=target)
		except Exception:
			raise IOError(errno.EEXIST, "%d:%s" % (self.nodeid,target))
		self.mtime = nowtuple()
		self.size += len(target)+1
		returnValue( oldnode ) # that's what's been linked, i.e. link count +=1
			

	@inlineCallbacks
	def mknod(self, name, mode, rdev, umask, ctx=None):
		with self.filesystem.db() as db:
			inode = yield self._new_inode(db,name,mode,ctx,rdev)
		returnValue( inode )

	@inlineCallbacks
	def mkdir(self, name, mode,umask, ctx=None):
		with self.filesystem.db() as db:
			inode = yield self._new_inode(db,name,(mode&0o7777&~umask)|stat.S_IFDIR,ctx)
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
			self.write_timer.cancel()
			self.write_timer = None

		entries = []
		def app(parent,size):
			entries.append((parent,name))
		yield db.DoSelect("select parent,name from tree where inode=${inode}", inode=self.nodeid, _empty=True, _callback=app)
		for p in entries:
			p,name = p
			p = SqlInode(self.filesystem,p)
			yield p._load(db)
			p.mtime = nowtuple()
			p.size -= len(name)+1
		yield db.Do("delete from tree where inode=${inode}", inode=self.nodeid, _empty=True)
		nnodes, = yield db.DoFn("select count(*) from node where root=${root}", root=self.filesystem.root_id)
		if nnodes == 1:
			yield db.Do("delete from inode where id=${inode}", inode=self.nodeid)
			yield db.call_committed(self.filesystem.rooter.d_inode,-1)
			if stat.S_ISREG(self.mode):
				yield db.call_committed(self.filesystem.rooter.d_size,self.size,0)
		elif stat.S_ISREG(self.mode):
			self.filesystem.record.delete(self)

		if stat.S_ISREG(self.mode):
			try: yield deferToThread(os.unlink,self._file_path())
			except EnvironmentError: pass
		self.nodeid = None
		returnValue( None )

	def __delete__(self):
		assert self.write_timer is None
		
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
			nid = yield self.filesystem.xattr_id(name,db,False)
			if nid is None:
				raise IOError(errno.ENOATTR)
			try:
				val, = yield db.DoFn("select value from xattr where inode=${inode} and name=${name}", inode=self.nodeid,name=nid)
			except NoData:
				raise IOError(errno.ENOATTR)
		returnValue( val )

	@inlineCallbacks
	def setxattr(self, name, value, flags, ctx=None):
		if len(value) > self.filesystem.info.attrlen:
			raise IOError(errno.E2BIG)
		with self.filesystem.db() as db:
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
			nid = self.filesystem.xattr_id(name, db,False)
			if nid is None:
				raise IOError(errno.ENOATTR)
			try:
				yield db.Do("delete from xattr where inode=${inode} and name=${name}", inode=self.nodeid,name=nid)
			except NoData:
				raise IOError(errno.ENOATTR)
		returnValue( None )

	def readlink(self, ctx=None):
		self.do_atime()
		return self.target

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
		ipath=self.node._file_path()
		if not os.path.exists(ipath) or self.mode & os.O_TRUNC:
			self.node.copies = 1 # ours
			self.node.size = 0
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
		self.node.do_atime()
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
		self.node.do_atime(is_dir=1)
		db = tree.db
		with tree.db() as db:
			if not offset:
				callback(".",self.node.nodeid,self.node.mode,0)
				if self.node.nodeid == self.node.filesystem.inum:
					callback("..",self.node.nodeid,self.node.mode,0)
				else:
					try:
						inum = yield db.DoFn("select '..',inode.id,inode.mode,0 from tree,inode where tree.inode=${inode} and tree.parent=inode.id limit 1", inode=self.node.nodeid)
					except NoData:
						pass
					else:
						callback(*inum)
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
