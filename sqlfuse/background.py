# -*- coding: utf-8 -*-

#    Copyright (C) 2010,2011  Matthias Urlichs <matthias@urlichs.de>
#
#    This program may be distributed under the terms of the GNU GPLv3.
#
## This file is formatted with tabs.
## Do NOT introduce leading spaces.

from __future__ import division, print_function, absolute_import

__all__ = ("RootUpdater","Recorder","NodeCollector","CacheRecorder","UpdateCollector","CopyWorker")

from collections import defaultdict
import os, sys
from traceback import print_exc

from zope.interface import implements

from twisted.application.service import Service,IService
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue, maybeDeferred, Deferred, DeferredList
from twisted.internet.threads import deferToThread
from twisted.python import log

from sqlmix import NoData
from sqlfuse.fs import BLOCKSIZE,SqlInode
from sqlfuse.range import Range
from sqlfuse.topo import next_hops
from sqlfuse.node import NoLink

class BackgroundJob(object,Service):
	"""\
		Do some work in the background. Abstract class; implement .work()
		and possibly change .interval.
		"""
	implements(IService)
	interval = 1.0
	worker = None
	workerRunning = None
	restart = False

	def __init__(self):
		self.setName(self.__class__.__name__)

	def startService(self):
		super(BackgroundJob,self).startService()
		if self.restart:
			self.run()

	def stopService(self):
		super(BackgroundJob,self).stopService()
		if self.worker:
			self.worker.cancel()
			self.worker = None
		return self.workerRunning

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
		self.workerRunning = maybeDeferred(self.work)
		def done(r):
			self.worker = None
			if self.restart and not self.running:
				self.trigger()
			self.workerRunning = None
			return r
		self.workerRunning.addErrback(lambda e: e.printTraceback(file=sys.stderr))
		self.workerRunning.addBoth(done)

	def work(self):
		raise NotImplementedError("You need to override %s.work" % (self.__class__.__name__))


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
	"""\
		I record inode changes so that another node can see what I did, and
		fetch my changes.
		"""
	def __init__(self,tree):
		super(Recorder,self).__init__()
		self.tree = tree
		self.data = []
		self.cleanup = reactor.callLater(10,self.cleaner)

	def cleaner(self):
		self.cleanup = None
		d = self._cleaner()
		def done(r):
			self.cleanup = reactor.callLater(10,self.cleaner)
			return r
		d.addBoth(done)
		d.addErrback(lambda r: r.printTraceback(file=sys.stderr))

	@inlineCallbacks
	def _cleaner(self):
		with self.tree.db() as db:
			try:
				all_done,n_nodes = yield db.DoFn("select min(event),count(*) from node where root=${root} and id != ${node}", root=self.tree.root_id, node=self.tree.node_id)
			except NoData:
				pass
			else:
				if not n_nodes:
					return
				try:
					last_syn, = yield db.DoFn("select id from event where node=${node} and typ = 's' and id <= ${evt} order by id desc limit 1", node=self.tree.node_id, evt=all_done)
					yield db.Do("delete from event where node=${node} and id < ${id}", id=last_syn, node=self.tree.node_id)
				except NoData:
					pass

	def quit(self):
		if self.cleanup:
			self.cleanup.cancel()
			self.cleanup = None
		super(Recorder,self).quit()

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


class CacheRecorder(BackgroundJob):
	"""\
		I record an inode's caching state.
		"""
	def __init__(self,tree):
		super(CacheRecorder,self).__init__()
		self.tree = tree
		self.caches = set()

	@inlineCallbacks
	def work(self):
		nn = self.caches
		self.caches = set()
		for n in nn:
			with self.tree.db() as db:
				if n.cache_id is None:
					n.cache_id = yield db.Do("insert into cache(cached,inode,node) values (${data},${inode},${node})", inode=n.nodeid, node=self.tree.node_id, data=n.known.encode())
				else:
					yield db.Do("update cache set cached=${data} where id=${cache}", cache=n.cache_id, data=n.known.encode(), _empty=True)

	def note(self,node):
		assert not isinstance(node,SqlInode)
		self.caches.add(node)
		self.trigger()
	

class NodeCollector(BackgroundJob):
	"""\
		Periodically try to make the internal list of remote nodes mirror
		the facts in the database.
		"""
	def __init__(self,tree):
		super(NodeCollector,self).__init__()
		self.tree = tree
		self.trigger()
		self.interval = 60
	
	@inlineCallbacks
	def work(self):
		nodes = set()
		self.restart = True
		with self.tree.db() as db:
			yield db.DoSelect("select id from node where id != ${node} and root = ${root}",root=self.tree.root_id, node=self.tree.node_id, _empty=1, _callback=nodes.add)

			# TODO: create a topology map
			## now create a topology map: how do I reach X from here?
			topo = yield self.get_paths(db)
			topo = next_hops(topo, self.tree.node_id)

		# drop obsolete nodes
		for k in self.tree.remote.keys():
			if k not in nodes:
				del self.tree.remote[k]

		#self.tree.topology = topo
		self.tree.topology,self.tree.neighbors = topo

		# add new nodes
		for k in nodes:
			if k not in self.tree.remote:
				d = self.tree.remote[k].connect_retry() # yes, this works, .remote auto-extends
				def pr(r):
					r.trap(NoLink)
					print("Node",k,"found, but no way to connect")
				d.addErrback(pr)
				d.addErrback(log.err)

				e = Deferred()
				d.addBoth(lambda r: e.callback(None))
				yield e


	
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


class UpdateCollector(BackgroundJob):
	"""\
		Periodically read the list of changes and update the list of
		updates that need to be processed.

		For each file node, this code looks at each change (recent changes
		first) and builds a list of remote and local changes. Processing
		stops when a file is truncated or deleted.

		Then, the changes are recorded in the 'processed' and 'cache' tables.
		"""
	def __init__(self,tree):
		super(UpdateCollector,self).__init__()
		self.tree = tree
		self.trigger()
		self.interval = 10 # TODO: production value should be lower
	
	@inlineCallbacks
	def work(self):
		nodes = set()
		self.restart = True
		with self.tree.db() as db:
			seq, = yield db.DoFn("select max(event.id) from event,node where typ='s' and node.root=${root} and node.id=event.node", root=self.tree.root_id)
			last,do_copy = yield db.DoFn("select event,autocopy from node where id=${node}", node=self.tree.node_id)
			if seq == last:
				return
			it = yield db.DoSelect("select event.id,event.inode,inode.typ,event.node,event.typ,event.range from event,node,inode where inode.id=event.inode and inode.typ='f' and event.id>${min} and event.id<=${max} and node.id=event.node and node.root=${root} order by event.inode,event.id desc", root=self.tree.root_id, min=last,max=seq, _empty=True)

			inode = None
			skip = False

			@inlineCallbacks
			def do_changed():
				if not inode:
					return
				if not data:
					return
				inode.cache.known.add_range(data)
				self.tree.changer.note(inode.cache)
				# TODO only do this if we don't just cache
				if do_copy:
					yield db.Do("replace into todo(node,inode,typ) values(${node},${inode},'f')", inode=inode.nodeid, node=self.tree.node_id, _empty=True)
					self.tree.copier.trigger()

			for i,inum,ityp,node,typ,r in it:
				if ityp != 'f':
					continue
				if not inode or inode.nodeid != inum:
					yield do_changed()
					inode = SqlInode(self.tree,inum)
					if inode.nodeid is not None: # locally deleted
						yield inode._load(db)
					data = Range()
					skip = False
				elif skip:
					continue

				if typ == 'i' or typ == 's' or typ == 'f':
					pass

				elif typ == 'c':
					r = Range(r)
					if node != self.tree.node_id:
						r = r.replace(self.tree.node_id,node)
					data.add_range(r,replace=False)

				elif typ == 'd':
					yield reactor.callInThread(self._os_unlink)
					skip = True

				elif typ == 'n':
					if data: size = data[-1][1]
					else: size=0
					yield deferToThread(inode.cache._trim, (inode.size if inode.size > size else size))
					inode.cache._trim(size)
					self.tree.changer.note(inode.cache)
					skip=True
					
				else:
					raise ValueError("event record %d: unknown type %s" % (i,repr(typ)))

			yield do_changed()
			yield db.Do("update node set event=${event} where id=${node}", node=self.tree.node_id, event=seq)
			
	
class CopyWorker(BackgroundJob):
	"""\
		Periodically read the TODO list and fetch the files mentioned there.
		"""
	def __init__(self,tree):
		super(CopyWorker,self).__init__()
		self.tree = tree
		self.trigger()
		self.interval = 10 # TODO: production value should be lower
		self.nfiles = 10
		self.last_entry = None
		self.workers = set()
	
	@inlineCallbacks
	def fetch(self,id,inode):
		self.workers.add(id)
		try:
			if inode.cache is not None:
				yield inode.cache.get_data(0,inode.size)
		except Exception as e:
			print_exc()
		else:
			with self.tree.db() as db:
				yield db.Do("delete from todo where id=${id}", id=id)
		finally:
			self.workers.remove(id)

	@inlineCallbacks
	def work(self):
		with self.tree.db() as db:
			f=""
			if self.last_entry:
				f = " and id > ${last}"

			entries = []
			def app(*a):
				entries.append(a)
			yield db.DoSelect("select id,inode,typ from todo where node=${node}"+f+" order by id limit ${nfiles}", node=self.tree.node_id,nfiles=self.nfiles,last=self.last_entry,_empty=True,_callback=app)
			if not entries:
				self.last_entry = None
				return
			self.restart = True

			defs = []
			for id,inum,typ in entries:
				if inum in self.workers:
					pass
				self.last_entry = id

				inode = SqlInode(self.tree,inum)
				if typ != 'f': # TODO: what about deletions?
					continue
				yield inode._load(db)
				defs.append(self.fetch(id,inode))
		yield DeferredList(defs)

		
