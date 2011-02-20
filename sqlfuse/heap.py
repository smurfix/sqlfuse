# -*- coding: utf-8 -*-

"""Heap class, because I dislike the procedural heapq"""

import heapq

class Heap(list):
	def __init__(heap, list = ()):
		for item in list:
			heapq.push(heap, item)
	def push(heap, item):
		heapq.heappush(heap, item)
	def pop(heap):
		return heapq.heappop(heap)
	def pushpop(heap, item):
		return heapq.heappushpop(heap, item)
	def replace(heap, item):
		return heapq.heapreplace(heap, item)

