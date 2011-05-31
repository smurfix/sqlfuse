# -*- coding: utf-8 -*-

#    Copyright (C) 2010,2011  Matthias Urlichs <matthias@urlichs.de>
#
#    This program may be distributed under the terms of the GNU GPLv3.
#
## This file is formatted with tabs.
## Do NOT introduce leading spaces.

from __future__ import division, print_function, absolute_import

__all__ = ("RootUpdater","Recorder","NodeCollector","CacheRecorder","UpdateCollector","CopyWorker","InodeCleaner","InodeWriter")

from collections import defaultdict
import errno,os,sys

from zope.interface import implements

from twisted.application.service import Service,IService
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue, maybeDeferred, Deferred, DeferredList, DeferredQueue
from twisted.internet.threads import deferToThread
from twisted.python import failure,log

from sqlmix import NoData
from sqlfuse import trace,tracer_info, triggeredDefer, NoLink, RemoteError
from sqlfuse.fs import BLOCKSIZE,SqlInode,DB_RETRIES,build_path
from sqlfuse.node import DataMissing
from sqlfuse.range import Range
from sqlfuse.topo import next_hops

tracer_info['background']="Start/stop of background processing"
# 'shutdown' is declared in sqlfuse.main

class IdleWorker(object,Service):
	"""\
		This is used when shutting down: run all workers continuously
		until none of them has more stuff to do.
		"""
	def __init__(self):
		self.workers = set()

	def add(self,w):
		if w.workerDefer:
			w.workerDefer.addCallback(lambda _: self.workers.add(w))
		else:
			self.workers.add(w)

	@inlineCallbacks
	def run(self):
		while self.workers:
			dl = []
			wl = self.workers
			self.workers = set()
			for w in list(wl):
				trace('shutdown',"IDLE start %s", w.__class__.__name__)
				w.restart = False
				d = maybeDeferred(w.work)
				def err_w(r,w):
					log.err(r,"Processing "+w.__class__.__name__)
					wl.remove(w)
				def log_w(r,w):
					trace('shutdown',"IDLE stop %s", w.__class__.__name__)
					return r
				d.addErrback(err_w,w)
				d.addBoth(log_w,w)
				dl.append(d)
			yield DeferredList(dl)
			trace('shutdown',"IDLE done")
			for w in wl:
				if w.restart:
					self.workers.add(w)
		
IdleWorker = IdleWorker()


class BackgroundJob(object,Service):
	"""\
		Do some work in the background. Abstract class; implement .work()
		and possibly change .interval.
		"""
	implements(IService)
	interval = 1.0
	workerCall = None # callLater
	workerDefer = None # Deferred which waits for the worker to end
	restart = False # if True, queue after initializing; if >1 start immediately
	do_idle = False

	def __init__(self,filesystem):
		self.fs = filesystem
		self.setName(self.__class__.__name__)
		self.interval += 5*self.fs.slow

	def startService(self):
		"""Startup. Part of IService."""
		trace('background',"StartService %s",self.__class__.__name__)
		super(BackgroundJob,self).startService()
		if self.restart and not self.workerCall:
			if self.restart > 1:
				self.run()
			else:
				self.trigger()

	def stopService(self):
		"""Shutdown. Part of IService. Triggers a last run of the worker."""
		trace('background',"StopService %s",self.__class__.__name__)
		try:
			self.do_idle = True
			super(BackgroundJob,self).stopService()
			if self.workerCall:
				self.workerCall.cancel()
				self.workerCall = None
			IdleWorker.add(self)
			d = triggeredDefer(self.workerDefer)
			if d is None:
				trace('background',"StopService %s: not running",self.__class__.__name__)
			else:
				trace('background',"StopService %s: wait for finish",self.__class__.__name__)
				def rep(r):
					trace('background',"StopService %s: finished",self.__class__.__name__)
					return r
				d.addBoth(rep)
			return d
		except Exception as e:
			log.err(e,"StopService "+self.__class__.__name__)

	def trigger(self, during_work=False):
		"""Tell the worker to do something. Sometime later, if possible."""
		if self.do_idle:
			trace('background',"Re-run %s (shutdown trigger)", self.__class__.__name__)
			IdleWorker.add(self)
			return
			
		if self.workerDefer is not None:
			if not self.restart:
				self.restart = True
		elif self.workerCall is None:
			if during_work:
				trace('background',"Start %s in %f sec (repeat)", self.__class__.__name__,self.interval)
			else:
				trace('background',"Start %s in %f sec", self.__class__.__name__,self.interval)
			if self.restart is True:
				self.workerCall = reactor.callLater(self.interval,self.run)
			else:
				self.workerCall = reactor.callLater(0,self.run)

	def run(self, dummy=None):
		"""Background loop. Started via timer from trigger()."""

		self.workerCall = None
		if self.workerDefer:
			if not self.restart:
				self.restart = True
			return
		self.restart = False
		trace('background',"Starting %s", self.__class__.__name__)
		self.workerDefer = maybeDeferred(self.work)
		def done(r):
			self.workerDefer = None
			if self.restart:
				if self.running:
					self.trigger(True)
				else:
					trace('background',"Stopped %s (shutdown)", self.__class__.__name__)
			else:
				trace('background',"Stopped %s", self.__class__.__name__)
		self.workerDefer.addErrback(lambda e: log.err(e,"Running "+self.__class__.__name__))
		self.workerDefer.addBoth(done)


	def work(self):
		"""Override this."""
		raise NotImplementedError("You need to override %s.work" % (self.__class__.__name__))


tracer_info['rootup']="BG: Log updates to node size info"

class RootUpdater(BackgroundJob):
	"""I update my root statistics (number of inodes and blocks) periodically, if necessary"""
	def __init__(self,filesystem):
		self.delta_inode = 0
		self.delta_dir = 0
		self.delta_block = 0
		super(RootUpdater,self).__init__(filesystem)

	def d_inode(self,delta):
		"""The number of my inodes changed."""
		trace('rootup',"#Inodes: %d", delta)
		self.delta_inode += delta
		self.trigger()

	def d_dir(self,delta):
		"""The number of directory inodes changed."""
		trace('rootup',"#Dirs: %d", delta)
		self.delta_dir += delta
		self.trigger()

	def d_size(self,old,new):
		"""A file size changed."""
		old = (old+BLOCKSIZE-1)//BLOCKSIZE
		new = (new+BLOCKSIZE-1)//BLOCKSIZE
		delta = new-old
		if not delta: return
		trace('rootup',"#Size: %d", delta)
		self.delta_block += delta
		self.trigger()

	@inlineCallbacks
	def work(self):
		"""Sync root data"""
		d_inode = self.delta_inode; self.delta_inode = 0
		d_dir = self.delta_dir; self.delta_dir = 0
		d_block = self.delta_block; self.delta_block = 0
		if d_inode or d_block or d_dir:
			trace('rootup',"sync i%d b%d d%d", d_inode, d_block, d_dir)
			yield self.fs.db(lambda db: db.Do("update root set nfiles=nfiles+${inodes}, nblocks=nblocks+${blocks}, ndirs=ndirs+${dirs}", root=self.fs.root_id, inodes=d_inode, blocks=d_block, dirs=d_dir), DB_RETRIES)
		returnValue( None )


tracer_info['inodeclean'] = "BG: delete no-longer-used inodes"

class InodeCleaner(BackgroundJob):
	"""\
		I delete inodes which have been deleted by my node
		when they're not used any more, i.e. all other nodes have caught up.
		"""
	interval = 30
	restart = True
	def __init__(self,filesystem):
		self.interval += 60*filesystem.slow
		super(InodeCleaner,self).__init__(filesystem)

	@inlineCallbacks
	def work(self):
		if self.running and not self.fs.single_node:
			self.restart = True

		@inlineCallbacks
		def do_work(db):
			inums = []
			try:
				all_done,n_nodes = yield db.DoFn("select min(event),count(*) from node where root=${root} and id != ${node}", root=self.fs.root_id, node=self.fs.node_id)
			except NoData:
				pass
			else:
				if not n_nodes:
					return
				trace('inodeclean',"%d nodes",n_nodes)
				try:
					last_syn, = yield db.DoFn("select id from event where node=${node} and typ = 's' and id <= ${evt} order by id desc limit 1", node=self.fs.node_id, evt=all_done)
				except NoData:
					pass
				else:
					# remove old inode entries
					trace('inodeclean',"upto %d",last_syn)

					yield db.DoSelect("select inode from event where node=${node} and typ = 'd' and id < ${id}", id=last_syn, node=self.fs.node_id, _callback=inums.append, _empty=True)
					yield db.Do("delete from event where node=${node} and id < ${id}", id=last_syn, node=self.fs.node_id, _empty=True)
			returnValue( inums )
		inums = yield self.fs.db(do_work, DB_RETRIES)

		# more deadlock prevention
		if inums:
			trace('inodeclean',"%d inodes",len(inums))
			yield self.fs.db(lambda db: db.Do("delete from inode where id in (%s)" % ",".join((str(x) for x in inums)), _empty=True), DB_RETRIES)


tracer_info['eventrecord'] = "BG: write event records"

class Recorder(BackgroundJob):
	"""\
		I record inode changes so that another node can see what I did.
		
		The other node will read these records and update their state.
		"""
	def __init__(self,filesystem):
		self.data = []
		super(Recorder,self).__init__(filesystem)

	@inlineCallbacks
	def work(self):
		"""Write change records, or a SYN if there were no new changes."""
		d = self.data
		self.data = []
		@inlineCallbacks
		def do_work(db):
			if not d:
				s = yield db.Do("insert into `event`(`inode`,`node`,`typ`,`range`) values(${inode},${node},${event},${data})", inode=self.fs.root_inum,node=self.fs.node_id,data=None,event='s' )
				trace('eventrecord',"write SYN %d",s)
			else:
				for event,inode,data in d:
					if not inode.inum:
						continue
					s = yield db.Do("insert into `event`(`inode`,`node`,`typ`,`range`) values(${inode},${node},${event},${data})", inode=inode.inum,node=self.fs.node_id,data=data,event=event )
					inode['event'] = s
				trace('eventrecord',"wrote %d records",len(d))
				self.restart = True # unconditional. That's OK, we want a SYN afterwards.
		yield self.fs.db(do_work, DB_RETRIES)
		
	def delete(self,inode):
		"""Record inode deletion"""
		trace('eventrecord',"delete inode %d",inode.inum)
		self.data.append(('d',inode,None))
		self.trigger()
	
	def new(self,inode):
		"""Record inode creation"""
		trace('eventrecord',"create inode %d",inode.inum)
		self.data.append(('n',inode,None))
		self.trigger()

	def change(self,inode,data):
		"""Record changed data"""
		trace('eventrecord',"update inode %d: %s",inode.inum,data)
		self.data.append(('c',inode,data))
		self.trigger()

	def trim(self,inode,size):
		"""Record changed inode size"""
		trace('eventrecord',"resize inode %d",inode.inum)
		self.data.append(('t',inode,None))
		self.trigger()

	def finish_write(self,inode):
		"""Record finished changing data, i.e. close-file. Obsolete."""
		trace('eventrecord',"finish inode %d",inode.inum)
		self.data.append(('f',inode,None))
		self.trigger()


tracer_info['cacherecord'] = "BG: write cache state records"

class CacheRecorder(BackgroundJob):
	"""\
		I record an inode's local caching state.
		"""
	def __init__(self,filesystem):
		self.caches = set()
		super(CacheRecorder,self).__init__(filesystem)

	def work(self):
		nn = self.caches
		self.caches = set()

		@inlineCallbacks
		def do_work(db):
			for n in nn:
				yield n._save(db)
		return self.fs.db(lambda db: do_work(db), DB_RETRIES)

	def note(self,node,event=None):
		"""Record that this inode's cache state needs to be written."""
		if event:
			trace('cacherecord',"update inode %d (%d)",node.inum,event)
			if node.write_event is None or node.write_event < event:
				node.write_event = event
		else:
			trace('cacherecord',"update inode %d",node.inum)
		self.caches.add(node)
		self.trigger()
	
	def now(self,node,event,db):
		"""Record that this inode's cache state needs to be written immediately."""
		trace('cacherecord',"update inode %d now (%d)",node.inum,event)
		if node.write_event is None or node.write_event < event:
			node.write_event = event
		db.call_rolledback(self.note,node)
		if node in self.caches:
			self.caches.remove(node)
		node._save(db,event=event)
	

tracer_info['inoderecord'] = "BG: write inode metadata updates"

class InodeWriter(BackgroundJob):
	"""\
		I write inode state.
		"""
	def __init__(self,filesystem):
		self.inodes = set()
		super(InodeWriter,self).__init__(filesystem)

	def work(self):
		nn = self.inodes
		self.inodes = set()

		@inlineCallbacks
		def do_work(db):
			for n in nn:
				if n.inum is None:
					return
				trace('inoderecord',"do write inode %d",n.inum)
				yield n._save(db)
		return self.fs.db(lambda db: do_work(db), DB_RETRIES)

	def note(self,node):
		"""Record that this inode's cache state needs to be written."""
		trace('inoderecord',"will write inode %d",node.inum)
		self.inodes.add(node)
		self.trigger()
	

tracer_info['nodecollect'] = "BG: collect other nodes and update internal state"

class NodeCollector(BackgroundJob):
	"""\
		Periodically read the list of external nodes from the database
		and update my local state.
		"""
	interval = 60
	restart = 2 # immediate
	def __init__(self,filesystem):
		self.interval += 90*filesystem.slow
		super(NodeCollector,self).__init__(filesystem)
	
	@inlineCallbacks
	def work(self):
		nodes = set()
		if self.running:
			trace('nodecollect',"Start collecting")
			self.restart = True
		else:
			trace('nodecollect',"Not collecting")
			return # don't need this when shutting down

		@inlineCallbacks
		def do_work(db):
			yield db.DoSelect("select id from node where id != ${node} and root = ${root}",root=self.fs.root_id, node=self.fs.node_id, _empty=1, _callback=nodes.add)
			if not nodes:
				trace('nodecollect',"No other nodes: shutting down collector")
				self.fs.single_node = True
				return
			self.fs.single_node = False
			trace('nodecollect',"%d other nodes",len(nodes))

			# TODO: create a topology map
			## now create a topology map: how do I reach X from here?
			topo = yield self.get_paths(db)
			topo = next_hops(topo, self.fs.node_id)
			trace('nodecollect',"topology %s",repr(topo))
			returnValue( topo )
		topo = yield self.fs.db(do_work, DB_RETRIES)

		# drop obsolete nodes
		for k in self.fs.remote.keys():
			if k not in nodes:
				trace('nodecollect',"drop node %s",k)
				del self.fs.remote[k]

		#self.fs.topology = topo
		self.fs.topology,self.fs.neighbors = topo

		# add new nodes
		self.fs.missing_neighbors = set()
		for k in nodes:
			if k not in self.fs.remote:
				trace('nodecollect',"add node %s",k)
				d = self.fs.remote[k].connect_retry() # yes, this works, .remote auto-extends
				def pr(r):
					r.trap(NoLink)
					trace('error',"Node %s found, but not connected",k)
				d.addErrback(pr)
				def lerr(r):
					log.err(r,"Problem adding node")
				d.addErrback(lerr)

				yield d
			if k not in self.fs.topology:
				self.fs.missing_neighbors.add(k)

		trace('nodecollect',"Done.")
		self.fs.cleaner.trigger()
		self.fs.copier.trigger()


	
	@inlineCallbacks
	def get_paths(self, db):
		"""\
			Read path tuples from the database
			"""
		ways = defaultdict(dict)
		def add_way(src,dest,distance,method):
			ways[src][dest] = distance
			if method == 'native' and src not in ways[dest]:
				ways[dest][src] = distance+1 # asymmetric default path
		yield db.DoSelect("select src,dest,distance,method from updater", _empty=1, _callback=add_way)
		returnValue( ways )


tracer_info['eventcollect'] = "BG: collect events from other nodes and write TODO records"

class UpdateCollector(BackgroundJob):
	"""\
		Periodically read the list of changes, and update the list of
		file updates that need to be processed.

		The system is read-only until processing has progressed to the
		latest sync record.
		"""
	interval = 2
	restart = True
	seq = None
	do_readonly = True
	
	@inlineCallbacks
	def work(self):
		nodes = set()
		if self.running:
			self.restart = True
		else:
			return # don't need this when shutting down

		@inlineCallbacks
		def do_work(db):
			seq = self.seq
			if seq is None:
				seq, = yield db.DoFn("select event.id from event where event.typ='s' and event.node in (select id from node where root=${root}) order by event.id desc limit 1", root=self.fs.root_id)
			last,do_copy = yield db.DoFn("select event,autocopy from node where id=${node}", node=self.fs.node_id)
			if seq == last:
				self.seq = None
				if self.do_readonly:
					self.do_readonly = False
					self.fs.readonly = False
					self.fs.copier.trigger()
				return

			if self.do_readonly and self.fs.readonly:
				upd = ""
				trace('eventcollect',"Events from %d to %d (initial sync)",last+1,seq)
			else:
				upd = " and node != ${node}"
				trace('eventcollect',"Events from %d to %d",last+1,seq)

			it = yield db.DoSelect("select event.id,event.inode,event.node,event.typ,event.range from event join inode on inode.id=event.inode where inode.typ='f' and event.id>${min} and event.id<=${max} and event.node in (select id from node where root=${root}"+upd+") order by event.id limit 100", root=self.fs.root_id, node=self.fs.node_id, min=last,max=seq, _empty=True)
			last = seq
			n=0
			for event,inum,node,typ,r in it:
				n+=1
				last = event
				if r:
					r = Range(r)
				trace('eventcollect',"%d: ev=%d node=%d typ=%s range=%s",inum,event,node,typ, "?" if r is None else str(r))
				if typ == 'i' or typ == 's' or typ == 'f':
					continue

				inode = SqlInode(self.fs,inum)
				if inode.inum is None:
					# locally deleted.
					# Temporarily create a fake cache record so that it'll be skipped when we continue.
					trace('eventcollect',"%s: deleted (ev=%d)", inode,eid)
					yield db.Do("replace into cache(inode,node,event) values(${inode},${node},${event})", inode=inum,node=self.fs.node_id,event=seq)
					continue
				yield inode._load(db)

				if typ == 'c':
					if node != self.fs.node_id:
						r = r.replace(self.fs.node_id,node)
					inode.cache.known.add_range(r,replace=True)

				elif typ == 't':
					yield db.Do("replace into todo(node,inode,typ) values(${node},${inode},'t')", inode=inode.inum, node=self.fs.node_id, _empty=True)
					continue

				elif typ == 'd':
					yield db.Do("replace into todo(node,inode,typ) values(${node},${inode},'d')", inode=inode.inum, node=self.fs.node_id, _empty=True)
					yield db.Do("delete from fail where node=${node} and inode=${inode}", inode=inode.inum, node=self.fs.node_id, _empty=True)
					continue

				elif typ == 'n':
					yield inode.cache.trim(0, do_file=False)
					if not do_copy:
						yield db.Do("replace into todo(node,inode,typ) values(${node},${inode},'t')", inode=inode.inum, node=self.fs.node_id, _empty=True)
					
				else:
					raise ValueError("event record %d: unknown type %s" % (event,repr(typ)))

				self.fs.changer.note(inode.cache,event)
				#self.fs.changer.now(inode.cache,event,db)

				# TODO only do this if we don't just cache
				if do_copy:
					trace('eventcollect',"TODO: inode %d",inode.inum)
					yield db.Do("replace into todo(node,inode,typ) values(${node},${inode},'f')", inode=inode.inum, node=self.fs.node_id, _empty=True)
					if not self.fs.readonly:
						db.call_committed(self.fs.copier.trigger)
				# yield inode.cache._close()

			yield db.Do("update node set event=${event} where id=${node}", node=self.fs.node_id, event=last, _empty=True)
			if n > 90:
				self.restart = 2

		yield self.fs.db(do_work, DB_RETRIES)
#			if self.fs.node_id == 3:
#				import sqlmix.twisted as t
#				t.breaker = True
			
	
tracer_info['copyrun'] = "BG: examine TODO records and copy files"

class CopyWorker(BackgroundJob):
	"""\
		Periodically read the TODO list and fetch the files mentioned there.

		This gets triggered by the UpdateCollector (via self.fs.copier).
		"""
	interval = 0.5
	def __init__(self,filesystem):
		self.nfiles = 1000
		self.nworkers = 10
		self.last_entry = None
		self.workers = set() # inode numbers
		super(CopyWorker,self).__init__(filesystem)
	
	@inlineCallbacks
	def fetch(self,db,queue):
		trace('copyrun',"job start")
		while self.running:
			i = yield queue.get()
			if not i:
				trace('copyrun',"job exit")
				return
			id,inode = i

			try:
				try:
					trace('copyrun',"%d: Start copying: %s",inode.inum,repr(inode.cache))
					if inode.cache is not None:
						res = yield inode.cache.get_data(0,inode.size)
						if self.running and not res:
							trace('copyrun',"%d: Data unavailable", inode.inum)
							yield db.Do("delete from todo where id=${id}", id=id, _empty=True)
							continue
					else:
						raise RuntimeError("inode %s: no cache"%(str(inode),))
				except NoLink as e:
					trace('copyrun',"Missing %d: %s",inode.inum, str(e.node_id) if e.node_id else "?")
					yield db.Do("replace into todo(node,inode,typ,missing) values(${node},${inode},'f',${missing})", node=self.fs.node_id,inode=inode.inum, missing=e.node_id)
					self.fs.missing_neighbors.add(e.node_id)
					continue
					
				except (RemoteError,DataMissing) as e:
					if isinstance(e,RemoteError) and e.type != 'sqlfuse.node.DataMissing':
						raise

					# Gone with the wind.
					inode.missing(0,inode.size)
					trace('copyrun',"%d: Data missing", inode.inum)
					yield db.Do("delete from todo where id=${id}", id=id, _Empty=True)
					continue

			except Exception:
				f = failure.Failure()
				trace('copyrun',"%d: Failure copying: %s",inode.inum,f)
				reason = f.getTraceback()
				try:
					yield db.Do("replace into fail(node,inode,reason) values(${node},${inode},${reason})", node=self.fs.node_id,inode=inode.inum, reason=reason)
				except Exception as ex:
					log.err(f,"Problem fetching file")
			else:
				trace('copyrun',"%d: Copy done: %s",inode.inum,repr(inode.cache))
				yield db.Do("delete from todo where id=${id}", id=id, _empty=True)
				yield db.Do("delete from fail where node=${node} and inode=${inode}", node=self.fs.node_id,inode=inode.inum, _empty=True)
#			finally:
#				yield inode.cache._close()
		trace('copyrun',"job exit2")

	@inlineCallbacks
	def work(self):
		"""\
			Fetch missing file( part)s.
			"""
		if not self.fs.topology:
			return

		@inlineCallbacks
		def do_work(db):
			f=""
			if self.last_entry:
				f = " and id > ${last}"
				trace('copyrun',"start TODO from %d",self.last_entry+1)
			else:
				trace('copyrun',"start TODO")
			if self.fs.missing_neighbors:
				trace('copyrun',"start missing %s",repr(self.fs.missing_neighbors))
				f += " and (missing is NULL or missing not in ("+(",".join((str(x) for x in self.fs.missing_neighbors)))+"))"

			entries = []
			def app(*a):
				entries.append(a)
			yield db.DoSelect("select id,inode,typ from todo where node=${node}"+f+" order by id limit ${nfiles}", node=self.fs.node_id,nfiles=self.nfiles,last=self.last_entry,_empty=True,_callback=app)
			returnValue( entries )

		if self.fs.readonly:
			return
		entries = yield self.fs.db(do_work, DB_RETRIES)
		if not entries:
			trace('copyrun',"no more items")
			self.last_entry = None
			return

		trace('copyrun',"%d items",len(entries))
		if self.running:
			self.restart = True

		@inlineCallbacks
		def do_work2(db):
			queue = DeferredQueue()
			defs = []
			nworkers = len(entries)//5+1
			if nworkers > self.nworkers:
				nworkers = self.nworkers

			for i in range(nworkers):
				d = self.fetch(db,queue)
				d.addErrback(log.err,"fetch()")
				defs.append(d)

			workers = set()
			for id,inum,typ in entries:
				if not self.running:
					break
				trace('copyrun',"%d: %s",inum,typ)

				self.last_entry = id

				if typ == 'd':
					def dt(inum):
						path = build_path(self.fs.store,inum, create=False)
						try:
							os.unlink(path)
						except EnvironmentError as e:
							if e.errno != errno.ENOENT:
								raise
					yield deferToThread(dt,inum)
				else:
					inode = SqlInode(self.fs,inum)
					yield inode._load(db)
					if typ == 'f':
						if inum in workers:
							trace('copyrun',"%d: in workers",inum,typ)
							continue
						workers.add(inum)
						queue.put((id,inode))
					elif typ == 't':
						if inode.cache:
							yield inode.cache.trim(inode.size)
					else:
						raise RuntimeError("Typ '%s' not found (inode %d)" % (typ,inum))
					continue

			for i in range(nworkers):
				queue.put(None)
			yield DeferredList(defs)
		yield self.fs.db(do_work2)
		trace('copyrun',"done until %s",self.last_entry)

		
