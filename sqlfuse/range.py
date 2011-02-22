# -*- coding: utf-8 -*-

#    Copyright (C) 2011  Matthias Urlichs <matthias@urlichs.de>
#
#    This program may be distributed under the terms of the GNU GPLv3.
#
## This file is formatted with tabs.
## Do NOT introduce leading spaces.

from __future__ import division, print_function, absolute_import

__all__ = ["Range"]

# This works out for small numbers. For large ones, swap them.
T="!\"$%&/()=?{[]}+*~#'-_.:,;<>\\^`@|0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
NT="abcdefghijklmnopqrstuvwxyz"

class Range(list):
	def __repr__(self):
		return "%s(%s)" % (self.__class__.__name__,list.__repr__(self))

	def encode(self,T=T,NT=NT):
		res = ""
		for aa in self:
			aa=(aa[0],aa[1]-aa[0])
			for a in aa:
				while a >= len(T):
					res += NT[a%len(NT)]
					a //= len(NT)
				res += T[a]
		return res

	def decode(self,s,T=T,NT=NT):
		a=None
		b=0
		mult=1
		for c in s:
			i = T.find(c)
			if i == -1:
				i = NT.index(c)
				b += mult*i
				mult *= len(NT)
				continue
			b += mult*i
			if a is None:
				a=b
			else:
				self.append((a,a+b))
				a=None
			b=0
			mult=1

		assert a is None
		assert b==0

	def add(self, start,end):
		"""Add a start/end range to the list"""
		assert end > start,"%d %d"%(start,end)
		a=0
		b=len(self)
		i=0
		while a<b:
			i=(a+b)//2
			if self[i][1] < start:
				a=i+1
				i=a
			elif self[i][0] >= start:
				b=i
			else:
				break
		while i < len(self) and self[i][0] <= end:
			s,e = self.pop(i)
			if start > s: start = s
			if end < e: end = e
		self.insert(i,(start,end))

if __name__ == "__main__":
	from random import SystemRandom
	import sys
	def run(start,end,res=None):
		a.add(start,end)
		r = a.encode()
		print("\trun(%d,%d,%r)" % (start,end,r))
		if res is not None and res != r:
			b = Range()
			b.decode(res)
			print("Want",repr(b))
			print("Has ",repr(a))
			print("after add",start,end)
			sys.exit(1)
	r=SystemRandom()
	a = Range()
	run(70,80,'s${')
	run(30,40,'@{s${')
	run(50,60,'@{I{s${')
	run(10,20,'{{@{I{s${')
	run(90,100,'{{@{I{s${m%{')
	run(9,10,'?[@{I{s${m%{')
	run(49,51,'?[@{H[s${m%{')
	run(87,89,'?[@{H[s${j%$m%{')
	run(41,43,'?[@{9$H[s${j%$m%{')
	run(20,22,'?}@{9$H[s${j%$m%{')
	run(59,61,'?}@{9$H]s${j%$m%{')
	run(5,33,'/39$H]s${j%$m%{')
	run(75,111,'/39$H]s$9')

#	for i in range(10000):
#		x=r.randint(2**15,2**20)
#		a.add(x,x+r.randint(2**6,2**10))
#	print(a)
#	print(a.encode())
#	for i in range(10000):
#		x=r.randint(2**25,2**30)
#		a.add(x,x+r.randint(2**16,2**20))
#	print(a)
#	s=a.encode()
#	print(s,len(s))
#	b = Range()
#	b.decode(s)
#	assert a == b

