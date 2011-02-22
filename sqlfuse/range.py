# -*- coding: utf-8 -*-

#    Copyright (C) 2011  Matthias Urlichs <matthias@urlichs.de>
#
#    This program may be distributed under the terms of the GNU GPLv3.
#
## This file is formatted with tabs.
## Do NOT introduce leading spaces.

from __future__ import division, print_function, absolute_import

__all__ = ["Range"]

class Range(list):
	def __repr__(self):
		return "%s(%s)" % (self.__class__.__name__,list.__repr__(self))

	def add(self, start,end):
		"""Add a start/end range to the list"""
		print("add",start,end)
		assert end > start
		a=0
		b=len(self)
		i=0
		while a<b:
			i=(a+b)//2
			if self[i][1] < start:
				a=i+1
			elif self[i][0] > end:
				b=i-1
			else:
				break
		i=a
		while i>0 and self[i-1][1] >= start:
			i -= 1
		while i<len(self) and self[i][1] < start:
			i += 1
		while i < len(self) and self[i][0] <= end:
			s,e = self.pop(i)
			if start > s: start = s
			if end < e: end = e
		self.insert(i,(start,end))

if __name__ == "__main__":
	a = Range()
	a.add(10,20)
	print(a)

	a.add(30,40)
	print(a)

	a.add(23,28)
	print(a)
	a.add(20,22)
	print(a)
	a.add(22,23)
	print(a)

	a.add(1,3)
	print(a)
	a.add(5,7)
	print(a)
	a.add(2,12)
	print(a)
	a.add(40,42)
	print(a)

