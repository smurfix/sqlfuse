# -*- coding: utf-8 -*-

#    Copyright (C) 2010,2011  Matthias Urlichs <matthias@urlichs.de>
#
#    This program may be distributed under the terms of the GNU GPLv3.
#
## This file is formatted with tabs.
## Do NOT introduce leading spaces.

from __future__ import division, print_function, absolute_import

__all__ = ("RootUpdater,Recorder")

import sys
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue, maybeDeferred

from sqlmix import NoData
from sqlfuse.fs import BLOCKSIZE

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


