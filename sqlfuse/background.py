# -*- coding: utf-8 -*-

#    Copyright (C) 2010,2011  Matthias Urlichs <matthias@urlichs.de>
#
#    This program may be distributed under the terms of the GNU GPLv3.
#
## This file is formatted with tabs.
## Do NOT introduce leading spaces.

from __future__ import division, print_function, absolute_import

__all__ = ("RootUpdater","Recorder","NodeCollector","CacheRecorder","UpdateCollector")

from collections import defaultdict
import os, sys
from traceback import print_exc

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue, maybeDeferred, Deferred
from twisted.internet.threads import deferToThread
from twisted.python import log

from sqlmix import NoData
from sqlfuse.fs import BLOCKSIZE,SqlInode
from sqlfuse.range import Range
from sqlfuse.topo import next_hops
from sqlfuse.node import NoLink

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
		rnodes = []
		with self.tree.db() as db:
			yield db.DoSelect("select id from node where root=${root} and id != ${node}", root=self.tree.root_id,node=self.tree.node_id, _empty=True, _callback=rnodes.append)
			if rnodes:
				pass
				# TODO
			else:
				# No nodes. Cleanup change records.
				try:
					evt, = yield db.DoFn("select id from event where node=${node} and typ = 's' order by id desc limit 1", node=self.tree.node_id)
					yield db.Do("delete from event where node=${node} and id < ${id}", id=evt,node=self.tree.node_id)
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
			last, = yield db.DoFn("select event from node where id=${node}", node=self.tree.node_id)
			if seq == last:
				return
			it = yield db.DoSelect("select event.id,event.inode,inode.typ,event.node,event.typ,event.range from event,node,inode where inode.id=event.inode and inode.typ='f' and event.id>${min} and event.id<=${max} and node.id=event.node and node.root=${root} order by event.inode,event.id desc", root=self.tree.root_id, min=last,max=seq, _empty=True)

			inode = None
			skip = False

			@inlineCallbacks
			def do_changed():
				if not inode:
					print("DC: no inode")
					return
				if not data:
					print("DC: no data for",inode.nodeid)
					return
				inode.cache.known.add_range(data)
				self.tree.changer.note(inode.cache)
				# TODO only do this if we don't just cache
				yield db.Do("replace into todo(node,inode,typ) values(${node},${inode},'f')", inode=inode.nodeid, node=self.tree.node_id, _empty=True)

			for i,inum,ityp,node,typ,r in it:
				print("DC: checking",i,inum)
				if ityp != 'f':
					print("DC: skip",ityp)
					continue
				if not inode or inode.nodeid != inum:
					print("DC: new node",inum)
					yield do_changed()
					inode = SqlInode(self.tree,inum)
					yield inode._load(db)
					data = Range()
					skip = False
				elif skip:
					print("DC: skipping")
					continue

				if typ == 'i' or typ == 's' or typ == 'f':
					print("DC: not interested",typ)
					pass

				elif typ == 'c':
					r = Range(r).replace(self.tree.node_id,node)
					print("DC: changed",r)
					data.add_range(r,replace=False)
					print("DC: data now",data)

				elif typ == 'd':
					print("DC: delete")
					try:
						yield reactor.callInThread(os.unlink,fn)
					except EnvironmentError as e:
						if e.errno != errno.ENOENT:
							print_exc()
					except Exception:
						print_exc()
					skip = True

				elif typ == 'n':
					print("DC: trim",inode.size)
					yield deferToThread(inode.cache._trim,inode.size)
					self.tree.changer.note(inode.cache)
					skip=True
					
				else:
					raise ValueError("event record %d: unknown type %s" % (i,repr(typ)))

			yield do_changed()
			yield db.Do("update node set event=${event} where id=${node}", node=self.tree.node_id, event=seq)
			print("DC: done")
			
