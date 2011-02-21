# -*- coding: utf-8 -*-

#    Copyright (C) 2010,2011  Matthias Urlichs <matthias@urlichs.de>
#
#    This program may be distributed under the terms of the GNU GPLv3.
#
## This file is formatted with tabs.
## Do NOT introduce leading spaces.

from __future__ import division, print_function, absolute_import

__all__ = ["SqlFuse"]

DBVERSION = "0.1"

BLOCKSIZE = 4096

import errno, fcntl, os, stat, sys, traceback
import inspect
from threading import Lock,Thread,Condition,RLock
from time import sleep,time
from weakref import WeakValueDictionary

# For deadlock debugging, I use a modified version of the threading module.
_Lock=Lock
_RLock=RLock
def Lock(name): return _Lock()
def RLock(name): return _RLock()
#import threading;threading._VERBOSE = True

import llfuse
from llfuse import FUSEError,lock_released

from sqlfuse.heap import Heap
from sqlfuse.options import options
from sqlmix import Db,NoData

inode_attrs = frozenset("size mode uid gid atime mtime ctime rdev".split())

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
	if flags & os.O_APPEND:
		mode = "a"
	else:
		mode = "w"
	f = (flags & (os.O_WRONLY|os.O_RDONLY|os.O_RDWR))
	if f == os.O_RDONLY:
		mode = "r"
	elif f == os.O_RDWR:
		mode += "+"

	return mode

class Info(object):
	def __init__(self,tree):
		for n,v in tree.db.DoSelect("select name,value from `info`"):
			setattr(self,n,v)
		

class Inode(object):
	"""\
	This represents an in-memory inode object.
	"""
	__slots__ = [ \
		'inum',          # inode number
		'seq',           # database update tracking
		'attrs',         # dictionary of current attributes (size etc.)
		'old_size',      # inode size, as stored in the database
		'updated',       # set of attributes not yet written to disk
		'seq',           # change sequence# on-disk
		'timestamp',     # time the attributes have been read from disk
		'tree',          # Operations object
		'lock',          # Lock for save vs. update
		'writelock',     # Lock for concurrent save
		'inuse',         # open files on this inode? <0:delete after close
		'_delay',        # defer updates (required for creation, which needs to be committed first)
		'__weakref__',   # required for the inode cache
		]

	def __repr__(self):
		if not self.inum:
			return "<Inode>"
		if not self.seq:
			return "<Inode %d>" % (self.inum)
		if not self.updated:
			return "<Inode %d:%d>" % (self.inum, self.seq)
		return "<Inode %d:%d (%s)>" % (self.inum, self.seq, " ".join(sorted(self.updated)))
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
		if isinstance(other,Inode):
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

	def __new__(cls, inum,tree, is_new=False):
		""""Return a cached copy, if possible"""
		if isinstance(inum,cls): return inum
		with tree.inode_cache_lock:
			try:
				return tree.inode_cache[inum]
			except KeyError:
				inode = object.__new__(cls)
				tree.inode_cache[inum] = inode
				inode.inum = None
				return inode

	def __init__(self,inum,tree, is_new=False):
		if isinstance(inum,Inode): return
		if self.inum is not None:
			assert self.inum == inum
			return
		self.inum = inum
		self.seq = None
		self.tree = tree
		self.lock = RLock(name="inode<%d>"%inum)
		self.writelock = Lock(name="w_inode<%d>"%inum)
		self.attrs = None
		self.timestamp = None
		self.inuse = 0
		self._delay = 0

		tree.add_cache(self)
		# defer anything we only need when loaded to after _load is called

	def _load(self):
		"""Load attributes from storage"""
		if not self.inum: return
		with self.lock:
			if self.seq:
				## TODO: also check the timestamp, don't load when recent enough
				seq, = self.tree.db.DoFn("select seq from inode where id=${inode}", inode=self.inum)
				if self.seq != seq:
					# Oops! somebody else changed the inode
					print("!!! inode_changed",self.inum,self.seq,seq,self.updated)
					self.seq = seq
					self._save()
				else:
					return

			self.attrs = {}
			self.updated = set()
			d = self.tree.db.DoFn("select * from inode where id=${inode}", inode=self.inum, _dict=1)
			for k in inode_attrs:
				if k.endswith("time"):
					v = (d[k],d[k+"_ns"])
				else:
					v = d[k]
				self.attrs[k] = v
			self.seq = d["seq"]
			self.old_size = d["size"]
			self.timestamp = nowtuple()
	
	def __getitem__(self,key):
		if not self.seq: self._load()
		return self.attrs[key]

	def __setitem__(self,key,value):
		if not self.seq: self._load()
		if key.endswith("time"):
			assert isinstance(value,tuple)
		else:
			assert not isinstance(value,tuple)
		if self.attrs[key] != value:
			with self.lock:
				if self.attrs[key] == value:
					return
				self.attrs[key] = value
				self.updated.add(key)
			self.tree.update_add(self)

	def _save(self):
		"""Save local attributes"""
		if not self.inum: return
		if self._delay: return

		# no "with" statement here: after collecting attrs, we need
		# to grab the writelock before releasing the inode lock
		self.lock.acquire()
		try:
			if not self.updated:
				self.lock.release()
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
		except:
			self.lock.release()
			raise

		with self.writelock:
			self.lock.release()

			try:
				self.tree.db.Do("update inode set seq=seq+1, "+(", ".join("%s=${%s}"%(k,k) for k in args.keys()))+" where id=${inode} and seq=${seq}", inode=self.inum, seq=self.seq, **args)
			except NoData:
				try:
					seq,self.old_size = self.tree.db.DoFn("select seq,size from inode where id=${inode}", inode=self.inum)
				except NoData:
					# deleted inode
					print("!!! inode_deleted",self.inum,self.seq,self.updated)
					self.inum = None
				else:
					# warn
					print("!!! inode_changed",self.inum,self.seq,seq,repr(args))
					self.tree.db.Do("update inode set seq=${seq}+1, "+(", ".join("%s=${%s}"%(k,k) for k in args.keys()))+" where id=${inode} and seq=${seq}", inode=self.inum, seq=seq, **args)
					self.seq = seq+1

			else:
				self.seq += 1
			if "size" in self.attrs and self.attrs["size"] != self.old_size:
				def do_size():
					with self.lock:
						self.tree.rooter.d_size(self.old_size,self.attrs["size"])
						self.old_size = self.attrs["size"]
				self.tree.db.call_committed(do_size)

# busy-inode flag
	def set_inuse(self):
		with self.lock:
			if self.inuse >= 0:
				self.inuse += 1
			else:
				self.inuse += -1

	def clr_inuse(self):
		#return True if it's to be deleted
		with self.lock:
			if self.inuse < 0:
				self.inuse += 1
				if self.inuse == 0:
					return True
			elif self.inuse > 0:
				self.inuse -= 1
			else:
				raise RuntimeError("Inode %r counter mismatch" % (self,))
			return False

	def defer_delete(self):
		with self.lock:
			if not self.inuse:
				return False
			if self.inuse > 0:
				self.inuse = -self.inuse
			return True
	def no_defer_delete(self,i):
		with self.lock:
			if self.inuse < 0:
				self.inuse = -self.inuse

	# background update (necessary after inode creation)
	def set_delay(self):
		self._delay = 1
		self.tree.db.call_committed(self.clr_delay)
		self.tree.db.call_rolledback(self.del_delay)

	def clr_delay(self):
		self._delay = 0
		self._save()
		
	def del_delay(self):
		self._delay = 0
		self.tree._forget(self)
		self.inum = None
		
def _setprop(key):
	def pget(self):
		return self[key]
	def pset(self,val):
		self[key] = val
	setattr(Inode,key,property(pget,pset))
for k in inode_attrs:
	_setprop(k)
del _setprop


class BackgroundJob(Thread):
	"""Updating inode information, particularly size+mtime, for each
	write() call is prohibitive. Therefore, metadata updates are accumulated,
	and each 0.5sec a background thread wakes up (if necessary) and
	writes all inode updates to the database.
	"""
	def __init__(self):
		super(BackgroundJob,self).__init__()
		self.lock = Condition(name=self.__class__.__name__)
		self.running = False

	def trigger(self, die=False):
		"""Tell the worker to do something."""
		with self.lock:
			if die:
				self.running = None
				self.lock.notify()
				return
			if not self.running:
				self.running = True
				self.lock.notify()

	def quit(self):
		"""Tell this piece of code to die."""
		self.trigger(True)
		self.join()
		self.work()


	def run(self):
		"""Background loop."""
		delay=0
		while True:
			with self.lock:
				while self.running is False:
					self.lock.wait(delay)
					if delay is not None: break
				if self.running is None:
					self.work()
					return
				self.running = False
			delay = self.work()

	def work(self):
		raise NotImplementedError("You need to override %s.work" % (self.__class__.__name__))

class BackgroundUpdater(BackgroundJob):
	def __init__(self,tree):
		super(BackgroundUpdater,self).__init__()
		self.tree = tree

	def work(self):
		"""Sync all inodes"""
		with self.tree._update_lock:
			u = self.tree._update
			self.tree._update = set()
		for inode in u:
			inode._save()
		self.tree.db.commit()
		sleep(0.5)
		return None

class RootUpdater(BackgroundJob):
	def __init__(self,tree):
		super(RootUpdater,self).__init__()
		self.tree = tree
		self.delta_inode = 0
		self.delta_dir = 0
		self.delta_block = 0

	def d_inode(self,delta):
		with self.lock:
			self.delta_inode += delta
			self.trigger()

	def d_dir(self,delta):
		with self.lock:
			self.delta_dir += delta
			self.trigger()

	def d_size(self,old,new):
		old = (old+BLOCKSIZE-1)//BLOCKSIZE
		new = (new+BLOCKSIZE-1)//BLOCKSIZE
		if old == new: return
		with self.lock:
			self.delta_block += new-old
			self.trigger()

	def work(self):
		"""Sync root data"""
		with self.lock:
			d_inode = self.delta_inode; self.delta_inode = 0
			d_dir = self.delta_dir; self.delta_dir = 0
			d_block = self.delta_block; self.delta_block = 0
		if d_inode or d_block or d_dir:
			self.tree.db.Do("update root set nfiles=nfiles+${inodes}, nblocks=nblocks+${blocks}, ndirs=ndirs+${dirs}", root=self.tree.root_id, inodes=d_inode, blocks=d_block, dirs=d_dir)
			self.tree.db.commit()
		sleep(0.5)
		return None

class InodeFlusher(BackgroundJob):
	def __init__(self):
		super(InodeFlusher,self).__init__()
		self.inodes = Heap()

	def add(self, inode):
		if not inode.inum: return
		with self.lock:
			self.inodes.push((time(),inode))
			if len(self.inodes) < 2:
				self.trigger()

	def work(self):
		t = time()-10
		while True:
			with self.lock:
				if not self.inodes:
					return None
				if self.inodes[0][0] > t:
					return self.inodes[0][0]-t
				inode = self.inodes.pop()[1]

class FileOperations(object):
	"""Operations on open files.
	"""
	mtime = None
	def __init__(self, inode, flags):
		"""Open the file. Also remember that the inode is in use
		so that delete calls get delayed.
		"""
		self.inode = inode
		self.lock = Lock(name="file<%d>"%inode.inum)
		mode = flag2mode(flags)
		self.mode = mode[0]
		self.file = open(inode.tree._file_path(inode),mode)
		inode.set_inuse()

	def read(self, length,offset):
		"""Read file, updating atime"""
		with self.lock:
			#self.inode.atime = nowtuple()
			self.file.seek(offset)
			return self.file.read(length)

	def write(self, buf,offset):
		"""Write file, updating file size, possibly updating size"""
		# TODO: use fstat() for size info, access may be concurrent
		with self.lock:
			self.file.seek(offset)
			self.file.write(buf)
			
			end = offset+len(buf)
			self.inode.mtime = nowtuple()
		with self.inode.lock:
			if self.inode.size < end:
				self.inode.size = end
		return len(buf)

	def release(self):
		"""The last process closes the file."""
		self.inode._save()
		self.file.close()
		if self.inode.clr_inuse():
			self.inode.tree._remove(self.inode)

	def _fflush(self):
		self.file.flush()

	def fsync(self, data_only):
		"""self-explanatory :-P"""
		if data_only and hasattr(os, 'fdatasync'):
			os.fdatasync(self.file.fileno())
		else:
			os.fsync(self.file.fileno())
			self.inode._save()

	def flush(self):
		"""One process closeds the file"""
		self._fflush()

	def lock(self, cmd, owner, **kw):
		log_call()
		raise FUSEError(errno.EOPNOTSUPP)
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
		op = { fcntl.F_UNLCK : fcntl.LOCK_UN,
			   fcntl.F_RDLCK : fcntl.LOCK_SH,
			   fcntl.F_WRLCK : fcntl.LOCK_EX }[kw['l_type']]
		if cmd == fcntl.F_GETLK:
			raise FUSEError(errno.EOPNOTSUPP)
		elif cmd == fcntl.F_SETLK:
			if op != fcntl.LOCK_UN:
				op |= fcntl.LOCK_NB
		elif cmd == fcntl.F_SETLKW:
			pass
		else:
			raise FUSEError(errno.EINVAL)

		fcntl.lockf(self.fd, op, kw['l_start'], kw['l_len'])

class DirOperations(object):
	mtime = None
	def __init__(self, inode):
		self.inode = inode
		self.inode.set_inuse()

	def read(self, offset):
		# We use the actual inode as offset, except for . and ..
		# Fudge the value a bit so that there's no cycle.
		tree = self.inode.tree
		db = tree.db
		if offset <= 1:
			yield (".", tree.getattr(self.inode), 2)
			offset = 3
		if offset <= 4:
			if self.inode.inum == tree.inum:
				par = self.inode
			else:
				par, = db.DoFn("select parent from tree where inode=${inode}", inode=self.inode.inum)
				par = Inode(par,tree)
			yield ("..", tree.getattr(par), 5)
			offset = 6

		for n,i in db.DoSelect("select name,inode from tree where parent=${par} and name != '' and inode > ${offset} order by inode", par=self.inode.inum,offset=(offset-10)//2, _empty=True,_store=True):
			yield (n, tree.getattr(Inode(i,tree)), 2*i+11)

	def release(self):
		if self.inode.clr_inuse():
			self.inode.tree._remove(self.inode)

	def fsync(self):
		log_call()
		return


class SqlFuse(llfuse.Operations):
	def __init__(self,*a,**k):
		super(SqlFuse,self).__init__(*a,**k)
		self._slot = {}
		self._slot_next = 1
		self._busy = {}
		self._busy_lock = Lock(name="busy")
		self._update = {}
		self._update_lock = Lock(name="update")
		self._xattr_name = {}
		self._xattr_id = {}
		self._xattr_lock = RLock(name="xattr") # protects name/id translation

		self.inode_cache = WeakValueDictionary()
		self.inode_cache_lock = Lock(name="inode_cache")
		self.inode_delay = Heap()


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

	def _file_path(self, inode):
		fp = []
		CHARS="ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		inode = inode.inum
		ino = inode % 100
		inode //= 100
		while inode:
			fp.insert(0,CHARS[inode % len(CHARS)])
			inode //= len(CHARS)
		p = os.path.join(self.store, *fp)
		if not os.path.exists(p):
			os.makedirs(p, 0o700)
		if ino < 10:
			ino = "0"+str(ino)
		else:
			ino = str(ino)
		return os.path.join(p, ino)

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

	def _lookup(self, inum_p, name):
		if name == '.':
			inum = inum_p
		elif name == '..':
			inum, = self.db.DoFn("select parent from tree where inode=${inode}",inode=inum_p)
		else:
			try:
				inum, = self.db.DoFn("select inode from tree where parent=${inode} and name=${name}", inode=inum_p, name=name)
			except NoData:
				raise(FUSEError(errno.ENOENT))
		return Inode(inum,self)
	    
	def lookup(self, inum_p, name):
		with lock_released:
			return self._getattr(self._lookup(inum_p,name))

	def handle_exc(*a,**k): #self,fn,exc):
		log_call()
		traceback.print_exc()
		self.db.rollback()

	def done(self, exc=None):
		if exc is None:
			self.db.commit()
		else:
			self.db.rollback()

	def getattr(self, inum):
		with lock_released:
			return self._getattr(inum)
	def _getattr(self, inum):
		inode = Inode(inum,self)
		res = llfuse.EntryAttributes()
		res.st_ino = inode
		res.st_nlink, = self.db.DoFn("select count(*) from tree where inode=${inode}",inode=inode.inum)
		for k in inode_attrs:
			if k.endswith("time"):
				v = inode[k]
				setattr(res,"st_"+k, v[0])
				setattr(res,"st_"+k+"_ns", v[1])
			else:
				setattr(res,"st_"+k, inode[k])
		if stat.S_ISDIR(res.st_mode): 
			res.st_nlink += 2
		res.st_ino=inode.inum
		res.st_blocks = (inode['size']+BLOCKSIZE-1)//BLOCKSIZE
		res.st_blksize = BLOCKSIZE
		res.st_dev=0
		res.st_rdev=0
		res.generation = 1 ## TODO: store in DB
		res.attr_timeout = 10
		res.entry_timeout = 10
		return res

	def setattr(self, inum, attr):
		with lock_released:
			inode = Inode(inum,self)
			if attr.st_size is not None:
				with file(self._file_path(inode),"r+") as f:
					f.truncate(attr.st_size)
			for f in inode_attrs:
				if f == "atime": continue
				v = getattr(attr,"st_"+f)
				if v is not None:
					if f.endswith("time"):
						v = (v, getattr(attr,"st_"+f+"_ns",0))
					setattr(inode,f,v)
			return self._getattr(inode)

	def readlink(self, inum):
		with lock_released:
			log_call()
			raise FUSEError(errno.EOPNOTSUPP)

	def opendir(self, inum):
		with lock_released:
			inode = Inode(inum,self)
			return self.new_slot(DirOperations(inode))

	def readdir(self, fh, offset):
		with lock_released:
			return self.old_slot(fh).read(offset)
		
	def fsyncdir(self, fh):
		with lock_released:
			log_call()
			return self.old_slot(fh).fsync()

	def releasedir(self, fh):
		with lock_released:
			return self.del_slot(fh).release()

	def unlink(self, inum_p, name):
		with lock_released:
			inode = self._lookup(inum_p,name)
			mode, = self.db.DoFn("select mode from inode where id=${inode}", inode=inode.inum)
			if stat.S_ISDIR(mode):
				raise FUSEError(errno.EISDIR)
			if not inode.defer_delete():
				self._remove(inode)

	def rmdir(self, inum_p, name):
		with lock_released:
			inode = self._lookup(inum_p,name)
			mode, = self.db.DoFn("select mode from inode where id=${inode}", inode=inode.inum)
			if not stat.S_ISDIR(mode):
				raise FUSEError(errno.ENOTDIR)
			cnt, = self.db.DoFn("select count(*) from tree where parent=${inode}", inode=inode.inum)
			if cnt:
				raise FUSEError(errno.ENOTEMPTY)
			self.db.call_committed(self.rooter.d_dir,-1)
			self._remove(inode)

	def _remove(self,inum):
		inode = Inode(inum,self)
		try:
			os.unlink(self._file_path(inode))
		except OSError as e:
			if e.errno != errno.ENOENT:
				raise
		mode,size = self.db.DoFn("select mode,size from inode where id=${inode}", inode=inode.inum)
		self.db.Do("delete from tree where inode=${inode}", inode=inode.inum)
		self.db.Do("delete from inode where id=${inode}", inode=inode.inum)
		self.db.call_committed(self.rooter.d_inode,-1)
		self.db.call_committed(self.rooter.d_size,size,0)

	def symlink(self, inum_p, name, target, ctx):
		with lock_released:
			log_call()
			raise FUSEError(errno.EOPNOTSUPP)

	def rename(self, inum_p_old, name_old, inum_p_new, name_new):
		with lock_released:
			log_call()
			raise FUSEError(errno.EOPNOTSUPP)

	def link(self, inum, inum_p_new, name_new):
		with lock_released:
			log_call()
			raise FUSEError(errno.EOPNOTSUPP)

	def mknod(self, inum_p, name, mode, rdev, ctx):
		with lock_released:
			log_call()
			inum = self._new_inode(inum_p,name,mode,ctx,rdev)
			return self._getattr(inum)

	def mkdir(self, inum_p, name, mode, ctx):
		with lock_released:
			inum = self._new_inode(inum_p,name,(mode&0o7777)|stat.S_IFDIR,ctx)
			self.db.call_committed(self.rooter.d_dir,1)
			return self._getattr(inum)

## not supported by llfuse
#	def bmap(self, *a,**k):
#		with lock_released:
#			log_call()
#			raise FUSEError(errno.EOPNOTSUPP)

	def forget(self, inum, nlookup):
		with lock_released:
			try:
				inode = self.inode_cache[inum]
			except KeyError:
				return
			with inode.lock:
				del self.inode_cache[inode]
				inode._save()
				inode.inum = None

	def add_cache(self,inode):
		assert self.inode_cache[inode.inum] == inode
		self.flusher.add(inode)
		
		

## not used, because the 'default_permissions' option is set
#	def access(self, inode, mode, ctx):
#		log_call()
#		raise FUSEError(errno.EOPNOTSUPP)


## xattr. The table uses IDs because they're much shorter than the names.

	#self._xattr_name = {}
	#self._xattr_id = {}
	#self._xattr_lock = Lock() # protects name/id translation

	def xattr_name(self,xid):
		"""xattr key-to-name translation"""
		try: return self._xattr_name[xid]
		except KeyError: pass

		self._xattr_lock.acquire()
		self.db.call_committed(self._xattr_lock.release)
		self.db.call_rolledback(self._xattr_lock.release)

		try: return self._xattr_name[xid]
		except KeyError: pass

		# this had better exist ...
		name, = self.db.DoFn("select name from xattr_name where id=${xid}",xid=xid)

		self._xattr_name[xid] = name
		self._xattr_id[name] = xid
		def _drop():
			del self._xattr_name[xid]
			del self._xattr_id[name]
		self.db.call_rolledback(_drop)

		return name

	def xattr_id(self,name):
		"""xattr name-to-key translation"""
		if len(name) == 0 or len(name) > self.info.attrnamelen:
			raise(FUSEError(errno.ENAMETOOLONG))
		try: return self._xattr_id[name]
		except KeyError: pass

		self._xattr_lock.acquire()
		self.db.call_committed(self._xattr_lock.release)
		self.db.call_rolledback(self._xattr_lock.release)

		try: return self._xattr_id[name]
		except KeyError: pass

		try:
			xid, = self.db.DoFn("select id from xattr_name where name=${name}", name=name)
		except NoData:
			xid = self.db.Do("insert into xattr_name(name) values(${name})", name=name)

		self._xattr_name[xid] = name
		self._xattr_id[name] = xid
		def _drop():
			del self._xattr_name[xid]
			del self._xattr_id[name]
		self.db.call_rolledback(_drop)

		return xid

	def getxattr(self, inum, name):
		with lock_released:
			nid=self.xattr_id(name)
			try:
				val, = self.db.DoFn("select value from xattr where inode=${inode} and name=${name}", inode=inum,name=nid)
			except NoData:
				raise(FUSEError(llfuse.ENOATTR))
			else:
				return val

	def setxattr(self, inum, name, value):
		with lock_released:
			if len(value) > self.info.attrlen:
				raise(FUSEError(errno.E2BIG))
			nid=self.xattr_id(name)
			inode = Inode(inum, self)
			with inode.lock: # protect against insert/insert or update/delete race
				try:
					self.db.Do("update xattr set value=${value},seq=seq+1 where inode=${inode} and name=${name}", inode=inum,name=nid,value=value)
				except NoData:
					self.db.Do("insert into xattr (inode,name,value,seq) values(${inode},${name},${value},1)", inode=inum,name=nid,value=value)

	def listxattr(self, inum):
		with lock_released:
			try:
				for nid, in self.db.DoSelect("select name from xattr where inode=${inode}", inode=inum):
					yield self.xattr_name(nid)
			except NoData:
				return

	def removexattr(self, inum, name):
		with lock_released:
			nid=self.xattr_id(name)
			inode = Inode(inum,self)
			with inode.lock: # protect against insert/insert or update/delete race
				try:
					self.db.Do("delete from xattr where inode=${inode} and name=${name}", inode=inum,name=nid)
				except NoData:
					raise(FUSEError(llfuse.ENOATTR))

	def statfs(self):
		"""\
		File system status.
		We recycle some values, esp. free space, from the underlying storage.
		"""
		with lock_released:
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

	def _new_inode(self, inum_p,name,mode,ctx,rdev=None):
		"""\
		Helper to create a new named inode.
		"""
		if len(name) == 0 or len(name) > self.info.namelen:
			raise FUSEError(errno.ENAMETOOLONG)
		now,now_ns = nowtuple()
		inum = self.db.Do("insert into inode (mode,uid,gid,atime,mtime,ctime,atime_ns,mtime_ns,ctime_ns,rdev) values(${mode},${uid},${gid},${now},${now},${now},${now_ns},${now_ns},${now_ns},${rdev})", mode=mode, uid=ctx.uid,gid=ctx.gid, now=now,now_ns=now_ns,rdev=rdev)
		self.db.Do("insert into tree (inode,parent,name) values(${inode},${par},${name})", inode=inum,par=inum_p,name=name)
		
		self.db.call_committed(self.rooter.d_inode,1)
		inode = Inode(inum,self)
		inode.set_delay()
		return inode

	def open(self, inum,flags):
		"""Existing file."""
		with lock_released:
			inode = Inode(inum,self)
			res = FileOperations(inode, flags)
			return self.new_slot(res)

	def create(self, inum_p,name,mode,ctx):
		"""New file."""
		with lock_released:
			try:
				inum, = self.db.DoFn("select inode from tree where parent=${par} and name=${name}", par=inum_p,name=name)
			except NoData:
				inode = self._new_inode(inum_p,name,mode|stat.S_IFREG,ctx)
			else:
				if flags & os.O_EXCL:
					raise FUSEError(errno.EEXIST)
				inode = Inode(inum,self)

			res = FileOperations(inode, os.O_RDWR|os.O_CREAT)
			return (self.new_slot(res),self._getattr(inode))

	def read(self, fh,off,size):
		"""Delegated to the file object."""
		with lock_released:
			return self.old_slot(fh).read(size,off)

	def write(self, fh,off,buf):
		"""Delegated to the file object."""
		with lock_released:
			return self.old_slot(fh).write(buf,off)

	def release(self, fh):
		"""Delegated to the file object."""
		with lock_released:
			return self.del_slot(fh).release()

	def fsync(self, fh,data_only): #isfsyncfile):
		"""Delegated to the file object."""
		with lock_released:
			return self.old_slot(fh).fsync(data_only)

	def flush(self, fh):
		"""Delegated to the file object."""
		with lock_released:
			self.old_slot(fh).flush()

	def lock(self, *a,**k): #cmd, owner):
		with lock_released:
			log_call()
			raise FUSEError(errno.EOPNOTSUPP)
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
#			raise FUSEError(errno.EOPNOTSUPP)
#		elif cmd == fcntl.F_SETLK:
#			if op != fcntl.LOCK_UN:
#				op |= fcntl.LOCK_NB
#		elif cmd == fcntl.F_SETLKW:
#			pass
#		else:
#			raise FUSEError(errno.EINVAL)
#
#		fcntl.lockf(self.fd, op, kw['l_start'], kw['l_len'])

	def init_db(self,db,node):
		"""\
		Setup the database part of the file system's operation.
		"""
		# TODO: setup a copying thread
		self.db = db
		self.node = node
		try:
			self.node_id,self.root_id,self.inum,self.store = self.db.DoFn("select node.id,root.id,root.inode,node.files from node,root where root.id=node.root and node.name=${name}", name=node)
		except NoData:
			raise RuntimeError("data for '%s' is missing"%(self.node,))
		try:
			mode,=db.DoFn("select mode from inode where id=${inode}",inode=self.inum)
		except NoData:
			raise RuntimeError("database has not been initialized: inode %d is missing" % (self.inode,))
		if mode == 0:
			db.Do("update inode set mode=${dir} where id=${inode}", dir=stat.S_IFDIR|stat.S_IRWXU|stat.S_IRWXG|stat.S_IRWXO, inode=self.inum)
		
		self.info = Info(self)
		if self.info.version != DBVERSION:
			raise RuntimeError("Need database version %s, got %s" % (DBVERSION,self.info.version))

	def update_add(self, inode):
		self._update.add(inode)
		self.syncer.trigger()

	def init(self):
		"""\
		Last step before running the file system mainloop.
		"""
		self.syncer = BackgroundUpdater(self)
		self.syncer.start()

		self.flusher = InodeFlusher()
		self.flusher.start()

		self.rooter = RootUpdater(self)
		self.rooter.start()

	def destroy(self):
		"""\
		Unmounting: tell the background job to stop.
		"""
		self.flusher.quit()
		self.flusher = None

		self.syncer.quit()
		self.syncer = None

		self.rooter.quit()
		self.rooter = None

		self.db.commit()
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

