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
	def sum(self):
		l = 0
		for s,e in self:
			l += e-s
		return l

	def __add__(self,other):
		"""Union"""
		res = Range(self)
		return res.__iadd__(other)
	def __iadd__(self,other):
		for a,b in other:
			self.add(a,b)
		return self

	def __sub__(self,other):
		"""Difference: Remove all parts which are in @other."""
		res = Range(self)
		return res.__isub__(other)
	def __isub__(self,other):
		for a,b in other:
			self.delete(a,b)
		return self

	def __and__(self,other):
		"""Intersection: Remove all parts which are not in @other."""
		res = Range(self)
		return res.__iand__(other)
	def __iand__(self,other):
		e = 0
		for a,b in other:
			if a > e: # test fails if the first range starts at zero
				self.delete(e,a)
			e = b
		if e is not None:
			while len(self):
				self[-1]
				if self[-1][0] >= e:
					self.pop()
				else:
					if self[-1][1] > e:
						self[-1] = (self[-1][0],e)
					break

		return self

	def equals(self,a,b):
		if a == b:
			return len(self) == 0

		if len(self) != 1:
			return False
		if self[0][0] != a or self[0][1] != b:
			return False
		return True

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
		chg = True
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
			if start >= s and end <= e:
				chg = False
			if start > s: start = s
			if end < e: end = e
		self.insert(i,(start,end))
		return chg

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
	def add(chg,start,end,res=None):
		c = a.add(start,end)
		r = a.encode()
		print("\tadd(%d,%d,%r)" % (start,end,r))
		if (res is not None and res != r) or chg != c:
			b = Range()
			b.decode(res)
			print("Want",chg,repr(b))
			print("Has ",c,repr(a),repr(a.encode()))
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
	add(1,70,80,'s${')
	add(1,30,40,'@{@{')
	add(1,50,60,'@{{{{{')
	add(1,11,19,'[=[{{{{{')
	add(0,11,19,'[=[{{{{{')
	add(1,10,19,'{?[{{{{{')
	add(1,10,20,'{{{{{{{{')
	add(0,10,19,'{{{{{{{{')
	add(0,11,20,'{{{{{{{{')
	add(1,90,100,'{{{{{{{{{{')
	add(1,9,10,'?[{{{{{{{{')
	add(1,49,51,'?[{{?[{{{{')
	add(1,87,89,'?[{{?[{{)$"{')
	add(1,41,43,'?[{{"$([{{)$"{')
	add(1,20,22,'?}={"$([{{)$"{')
	add(1,59,61,'?}={"$(]?{)$"{')
	add(1,5,33,'/3"$(]?{)$"{')
	add(1,75,111,'/3"$(]?9')
	rem(20,30,'/*{{"$(]?9')
	rem(80,90,'/*{{"$(]?{{.')
	rem(40,60,'/*{{_"?{{.')
	rem(2,5,'/*{{_"?{{.')
	rem(2,6,'(+{{_"?{{.')
	rem(111,115,'(+{{_"?{{.')
	rem(110,115,'(+{{_"?{{_')
	rem(19,32,'(}}=_"?{{_')
	rem(29,31,'(}}=_"?{{_')
	#print(repr(a))

	def t(a,b,op,c):
		a=Range(a)
		b=Range(b)
		c=Range(c)
		if getattr(a,'__'+op+'__')(b) != c:
			print(a,op,b,"!=",c)
			raise RuntimeError

	t(((10,20),),((30,40),),'add',((10,20),(30,40)))
	t(((10,20),),((30,40),),'sub',((10,20),))
	t(((10,80),),((30,40),),'sub',((10,30),(40,80)))
	t(((10,80),),((30,40),),'and',((30,40),))
	t(((10,40),),((30,40),),'and',((30,40),))
	t(((10,40),),((30,41),),'and',((30,40),))
	t(((10,41),),((30,40),),'and',((30,40),))
	t(((10,30),),((30,40),),'and',())
	t(((10,31),),((30,40),),'and',((30,31),))
	t(((10,31),(98,99)),((30,40),),'and',((30,31),))
	print("All tests OK.")

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

