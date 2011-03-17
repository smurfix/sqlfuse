# -*- coding: utf-8 -*-

#    Copyright (C) 2010,2011  Matthias Urlichs <matthias@urlichs.de>
#
#    This program may be distributed under the terms of the GNU GPLv3.
#
## This file is formatted with tabs.
## Do NOT introduce leading spaces.

from __future__ import division, print_function, absolute_import

__all__ = ("next_hops",)

from collections import defaultdict #,OrderedDict ## Py2.7
import sys

def next_hops(topo, node):
	"""\
		Given a source→destination→distance map and a starting node,
		return a distance-sorted list of destination→nexthop tuples.

		The input map is destroyed, so copy it if you want to re-use it.

		This code uses the Dijkstra algorithm.

		>>> topo = {1:{2:1,3:5,4:30,5:100},2:{1:9,3:1,4:30},3:{4:10},4:{5:10}}
		>>> generate_map(topo.copy(),2)
		[(3, 3), (1, 1), (4, 3), (5, 3)]
		>>> 
		"""
	distance = {}
	source = {}
	link = defaultdict(dict)
	done = set()

	for n in topo.keys():
		distance[n] = None
	distance[node] = 0
	#res = OrderedDict()
	resm = {}
	resl = []

	# ideally we'd use a heap to find the smallest item, but the list is
	# usually rather small and heapq doesn't have a configurable sort
	# method, so why bother
	def min_dist():
		n=None
		nd = None
		for k in topo.keys():
			ndk = distance[k]
			if nd is None or ndk is not None and nd > ndk:
				n = k
				nd = ndk
		return n,nd

	while topo:
		n,dist = min_dist()
		done.add(n)
		if dist is not None: # reachable?
			for dest,d in topo[n].items():
				if dest not in topo and dest not in done:
					topo[dest] = {}
				if distance.get(dest,None) is None or dist+d < distance[dest]:
					distance[dest] = dist+d
					if n in source and source[n] in source:
						source[dest] = source[n]
					else:
						source[dest] = dest
		if n != node and n in source:
			#res[n] = source[n]
			resm[n] = source[n]
			resl.append(n)
		del topo[n]

	return resm,resl


	pass

if __name__ == '__main__':
	topo = {1:{2:1,3:5,4:30,5:100},2:{1:9,3:1,4:30},3:{4:10},4:{5:10}}
	print ("TOPO",topo)
	for i in range(5):
		print ("PATH",i+1,next_hops(topo.copy(),i+1))

