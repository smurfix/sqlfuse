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
	"""\
	This class represents a disjunct sorted range of integers,
	stored as (begin,end) tuples.
	"""
	def __init__(self,data=None, T=T,NT=NT):
		"""If a string is given, decode it"""
		if isinstance(data,basestring):
			super(Range,self).__init__()
			self.decode(data)
		elif data is not None:
			super(Range,self).__init__(data)
		else:
			super(Range,self).__init__()

	def __repr__(self):
		return "%s(%s)" % (self.__class__.__name__,list.__repr__(self))
	def __str__(self):
		return ",".join(((("%d-%d"%(a,b-1)) if a<b else str(a)) for a,b in self))

	def encode(self,T=T,NT=NT):
		"""\
		Create a compact encoding of a range. This encoder uses seperate
		symbols for terminal vs. nonterminal numeric values,
		thereby folding the delimiter into the encoding and reducing
		code size for smaller numeric ranges. Only encoding differences
		further decreases code size.
		"""
		res = ""
		off=0
		for aa in self:
			noff=aa[1]
			aa=(aa[0]-off,aa[1]-aa[0])
			for a in aa:
				while a >= len(T):
					res += NT[a%len(NT)]
					a //= len(NT)
				res += T[a]
			off = noff
		return res

	def decode(self,s,T=T,NT=NT):
		"""\
		Add the range encoded in `s` to the current range.
		This is the inverse of the 'encode' method.
		"""
		a=None
		b=0
		mult=1
		off=0
		if len(self): # since there are existing entries, go slow
			add = self.add
		else: # faster, since input is serial
			def add(a,b): self.append((a,b))

		for c in s:
			i = T.find(c)
			if i == -1:
				i = NT.index(c)
				b += mult*i
				mult *= len(NT)
				continue
			b += mult*i + off
			off = b
			if a is None:
				a=b
			else:
				add(a,a+b)
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

	def delete(self, start,end):
		"""Remove a start/end range to the list"""
		assert end > start,"%d %d"%(start,end)
		a=0
		b=len(self)
		i=0
		while a<b:
			i=(a+b)//2
			if self[i][1] <= start:
				a=i+1
				i=a
			elif self[i][0] > start:
				b=i
			else:
				break
		if i < len(self):
			if i < len(self) and start > self[i][0] and end < self[i][1]:
				b1 = (self[i][0],start)
				b2 = (end,self[i][1])
				self[i] = b2
				self.insert(i,b1)
				return
			if self[i][0] < start:
				assert self[i][1] > start
				self[i] = (self[i][0],start)
				i += 1
			while i < len(self) and self[i][1] <= end:
				self.pop(i)
			if i < len(self) and self[i][0] < end:
				self[i] = (end,self[i][1])

if __name__ == "__main__":
	"""Test the module."""
	from random import SystemRandom
	import sys
	def add(start,end,res=None):
		a.add(start,end)
		r = a.encode()
		print("\tadd(%d,%d,%r)" % (start,end,r))
		if res is not None and res != r:
			b = Range()
			b.decode(res)
			print("Want",repr(b))
			print("Has ",repr(a),repr(a.encode()))
			print("after add",start,end)
			sys.exit(1)
	def rem(start,end,res=None):
		old = repr(a)
		a.delete(start,end)
		r = a.encode()
		print("\trem(%d,%d,%r)" % (start,end,r))
		if res is not None and res != r:
			b = Range()
			b.decode(res)
			print("Want",repr(b))
			print("Has ",repr(a),repr(a.encode()))
			print("del",start,end,"from",old)
			sys.exit(1)
	r=SystemRandom()
	a = Range()
	add(70,80,'s${')
	add(30,40,'@{@{')
	add(50,60,'@{{{{{')
	add(10,20,'{{{{{{{{')
	add(90,100,'{{{{{{{{{{')
	add(9,10,'?[{{{{{{{{')
	add(49,51,'?[{{?[{{{{')
	add(87,89,'?[{{?[{{)$"{')
	add(41,43,'?[{{"$([{{)$"{')
	add(20,22,'?}={"$([{{)$"{')
	add(59,61,'?}={"$(]?{)$"{')
	add(5,33,'/3"$(]?{)$"{')
	add(75,111,'/3"$(]?9')
	rem(20,30,'/*{{"$(]?9')
	rem(80,90,'/*{{"$(]?{{.')
	rem(40,60,'/*{{_"?{{.')
	rem(2,5,'/*{{_"?{{.')
	rem(2,6,'(+{{_"?{{.')
	rem(111,115,'(+{{_"?{{.')
	rem(110,115,'(+{{_"?{{_')
	rem(19,32,'(}}=_"?{{_')
	rem(29,31,'(}}=_"?{{_')
	print(repr(a))

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

