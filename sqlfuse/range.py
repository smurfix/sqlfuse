# -*- coding: utf-8 -*-

#    Copyright (C) 2011  Matthias Urlichs <matthias@urlichs.de>
#
#    This program may be distributed under the terms of the GNU GPLv3.
#
## This file is formatted with tabs.
## Do NOT introduce leading spaces.

from __future__ import division, print_function, absolute_import

from twisted.spread.flavors import Copyable
from twisted.spread.jelly import globalSecurity

__all__ = ["Range"]

# This works out for small numbers. For large ones, swap them.
T="!\"$%&/()=?{[]}+*~#'-_.:,;<>\\^`@|0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
NT="abcdefghijklmnopqrstuvwxyz"

class NotGiven: pass
class Range(list,Copyable):
	"""\
	This class represents a disjunct sorted range of integers,
	stored as (begin,end) tuples.
	"""
	def __init__(self,data=None, T=T,NT=NT):
		"""If a string is given, decode it"""
		if isinstance(data,basestring):
			super(Range,self).__init__()
			self.decode(data)
		elif isinstance(data,Range):
			super(Range,self).__init__(data)
		else:
			super(Range,self).__init__()
			if data is not None:
				for x in data:
					self.add(*x)

	def __repr__(self):
		return "%s(%s)" % (self.__class__.__name__,list.__repr__(self))
	def __str__(self):
		if len(self):
			def pr(a,b,c=None):
				res = str(a)
				if a < b-1:
					res += "-"+str(b-1)
				if c is not None:
					res += ":"+str(c)
				return res

			return ",".join(pr(*x) for x in self)
		else:
			return "-"

	# Twisted definitely has way too many state reading and writing function names
	def getStateToCopy(self):
		return self.__getstate__()
	def getStateFor(self,j):
		return self.__getstate__()
	def __getstate__(self):
		return self.encode()

	def setStateFor(self,j,s):
		self.__setstate__(s)
	def __setstate__(self,s):
		self.decode(s)
		
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
		for a,b,c in other:
			self.add(a,b,c)
		return self

	def __sub__(self,other):
		"""Difference: Remove all parts which are in @other."""
		res = Range(self)
		return res.__isub__(other)
	def __isub__(self,other):
		for a,b,c in other:
			self.delete(a,b)
		return self

	def __and__(self,other):
		"""Intersection: Remove all parts which are not in @other."""
		res = Range(self)
		return res.__iand__(other)
	def __iand__(self,other):
		e = 0
		for a,b,c in other:
			if a > e: # test fails if the first range starts at zero
				self.delete(e,a)
			e = b
		if e is not None:
			while len(self):
				if self[-1][0] >= e:
					self.pop()
				else:
					if self[-1][1] > e:
						self[-1] = (self[-1][0],e,self[-1][2])
					break

		return self

	def equals(self,a,b,c=NotGiven):
		if a == b:
			return len(self) == 0

		if len(self) != 1:
			return False
		if self[0][0] != a or self[0][1] != b or (c is not NotGiven and self[0][1] != c):
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
		for a,b,c in self:
			noff=b
			aa=(a-off, b-a, (c if c is not None else 0))
			for x in aa:
				while x >= len(T):
					res += NT[x%len(NT)]
					x //= len(NT)
				res += T[x]
			off = noff
		return res

	def decode(self,s,T=T,NT=NT):
		"""\
		Add the range encoded in `s` to the current range.
		This is the inverse of the 'encode' method.
		"""
		a=None
		b=None
		x=0
		mult=1
		off=0
		if len(self): # since there are existing entries, go slow
			add = self.add
		else: # faster, since input is serial
			def add(a,b,c):
				self.append((a,b,c))

		for c in s:
			i = T.find(c)
			if i == -1:
				i = NT.index(c)
				x += mult*i
				mult *= len(NT)
				continue
			x += mult*i + off
			if a is None:
				off = x
				a = x
			elif b is None:
				off = x
				b = a+x
			else:
				if x == 0:
					x = None
				add(a,b,x)
				a=None
				b=None
			x=0
			mult=1

		assert a is None

	def add(self, start,end, cause=None, replace=True):
		"""Add a start/end range to the list"""
		if start == end: return False
		assert end > start,"%d %d"%(start,end)
		a=0
		b=len(self)
		i=0
		chg = False
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
			# we may extend this range
			a,b,c = self[i]
			if a <= start and end <= b and (cause == c or not replace):
				return chg
			if cause == c:
				# Compatible range. Simply extend.
				if start < a:
					chg = True
				elif start > a:
					start = a
				if end > b:
					chg = True
				elif end < b:
					end = b
				self.pop(i)
				continue
			if replace:
				chg = True
			if a < start:
				# need to keep some original data

				assert start <= b # assured by binsearch, above
				if replace:
					self[i] = (a,start,c)
				else:
					if end <= b:
						return chg # done
					start = b
				i += 1
				continue
			if start < a: # empty area before the next block
				if end <= a:
					break
				if replace:
					if b <= end: # eat the block
						self.pop(i)
						continue
					# keep the block's end
					self[i] = (end,b,c)
					break
				else:
					self.insert(i,(start,a,cause))
					i += 1
					chg = True
					if end > b:
						start = b
						continue
					return chg

			assert a==start
			if replace:
				if b <= end:
					self.pop(i)
					continue
				self[i] = (end,b,c)
				break
			else:
				if b < end:
					start = b
					i += 1
					continue
				return chg
			i += 1
		if start < end:
			self.insert(i,(start,end,cause))
			chg = True
		return chg

	def delete(self, start,end):
		"""Remove a start/end range to the list"""
		if start == end: return
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
			if self[i][0] < start:
				b1 = (self[i][0],start,self[i][2])
				if end < self[i][1]:
					# we're in the middle of a range, thus need to split it
					b2 = (end,self[i][1],self[i][2])
					self[i] = b1
					self.insert(i+1,b2)
					return
				# we start within a range, but extend past its end
				self[i] = b1
				i += 1
			while i < len(self) and self[i][1] <= end:
				# we cover this range
				self.pop(i)
			if i < len(self) and self[i][0] < end:
				# we end within this range
				self[i] = (end,self[i][1],self[i][2])

globalSecurity.allowInstancesOf(Range)

if __name__ == "__main__":
	"""Test the module."""
	from random import SystemRandom
	import sys
	def add(rep,c,chg,start,end,res=None):
		ch = a.add(start,end,c,rep)
		r = str(a)
		print("\tadd(%d,%s,%d,%d,%d,%r)" % (rep, repr(c),chg,start,end,r))
		if (res is not None and res != r) or chg != ch:
			print("Want",chg,res)
			print("Has ",ch,r)
			sys.exit(1)

	def rem(start,end,res=None):
		a.delete(start,end)
		r = str(a)
		print("\trem(%d,%d,%r)" % (start,end,str(a)))
		if res is not None and res != r:
			print("Want",res)
			print("Has ",r)
			sys.exit(1)
	r=SystemRandom()
	a = Range()
	add(0,None,1,70,80,'70-79')
	add(0,None,1,30,40,'30-39,70-79')
	add(0,None,1,50,60,'30-39,50-59,70-79')
	add(0,None,1,11,19,'11-18,30-39,50-59,70-79')
	add(0,None,0,11,19,'11-18,30-39,50-59,70-79')
	add(0,None,1,10,19,'10-18,30-39,50-59,70-79')
	add(0,None,1,10,20,'10-19,30-39,50-59,70-79')
	add(0,None,0,10,19,'10-19,30-39,50-59,70-79')
	add(0,None,0,11,20,'10-19,30-39,50-59,70-79')
	add(0,None,1,90,100,'10-19,30-39,50-59,70-79,90-99')
	add(0,None,1,9,10,'9-19,30-39,50-59,70-79,90-99')
	add(0,None,1,49,51,'9-19,30-39,49-59,70-79,90-99')
	add(0,None,1,87,89,'9-19,30-39,49-59,70-79,87-88,90-99')
	add(0,None,1,41,43,'9-19,30-39,41-42,49-59,70-79,87-88,90-99')
	add(0,None,1,20,22,'9-21,30-39,41-42,49-59,70-79,87-88,90-99')
	add(0,None,1,59,61,'9-21,30-39,41-42,49-60,70-79,87-88,90-99')
	add(0,None,1,5,33,'5-39,41-42,49-60,70-79,87-88,90-99')
	add(0,None,1,75,111,'5-39,41-42,49-60,70-110')
	add(0,None,1,60,70,'5-39,41-42,49-110')
	rem(20,30,'5-19,30-39,41-42,49-110')
	rem(80,90,'5-19,30-39,41-42,49-79,90-110')
	rem(39,60,'5-19,30-38,60-79,90-110')
	rem(2,5,'5-19,30-38,60-79,90-110')
	rem(2,6,'6-19,30-38,60-79,90-110')
	rem(111,115,'6-19,30-38,60-79,90-110')
	rem(110,115,'6-19,30-38,60-79,90-109')
	rem(18,32,'6-17,32-38,60-79,90-109')
	rem(29,31,'6-17,32-38,60-79,90-109')

	a = Range()
	add(1,1,1,70,80,'70-79:1')
	add(1,2,1,30,40,'30-39:2,70-79:1')
	add(1,3,1,50,60,'30-39:2,50-59:3,70-79:1')
	add(1,None,1,11,19,'11-18,30-39:2,50-59:3,70-79:1')
	add(1,5,1,11,19,'11-18:5,30-39:2,50-59:3,70-79:1')
	add(1,6,1,10,19,'10-18:6,30-39:2,50-59:3,70-79:1')
	add(1,7,1,10,20,'10-19:7,30-39:2,50-59:3,70-79:1')
	add(1,8,1,10,19,'10-18:8,19:7,30-39:2,50-59:3,70-79:1')
	add(1,9,1,11,20,'10:8,11-19:9,30-39:2,50-59:3,70-79:1')
	add(1,10,1,90,100,'10:8,11-19:9,30-39:2,50-59:3,70-79:1,90-99:10')
	add(1,11,1,9,10,'9:11,10:8,11-19:9,30-39:2,50-59:3,70-79:1,90-99:10')
	add(1,12,1,49,51,'9:11,10:8,11-19:9,30-39:2,49-50:12,51-59:3,70-79:1,90-99:10')
	add(1,None,1,87,89,'9:11,10:8,11-19:9,30-39:2,49-50:12,51-59:3,70-79:1,87-88,90-99:10')
	add(1,14,1,41,43,'9:11,10:8,11-19:9,30-39:2,41-42:14,49-50:12,51-59:3,70-79:1,87-88,90-99:10')
	add(1,15,1,20,22,'9:11,10:8,11-19:9,20-21:15,30-39:2,41-42:14,49-50:12,51-59:3,70-79:1,87-88,90-99:10')
	add(1,16,1,59,61,'9:11,10:8,11-19:9,20-21:15,30-39:2,41-42:14,49-50:12,51-58:3,59-60:16,70-79:1,87-88,90-99:10')
	add(1,17,1,5,33,'5-32:17,33-39:2,41-42:14,49-50:12,51-58:3,59-60:16,70-79:1,87-88,90-99:10')
	add(1,18,1,75,111,'5-32:17,33-39:2,41-42:14,49-50:12,51-58:3,59-60:16,70-74:1,75-110:18')
	add(1,19,1,60,70,'5-32:17,33-39:2,41-42:14,49-50:12,51-58:3,59:16,60-69:19,70-74:1,75-110:18')
	rem(20,30,'5-19:17,30-32:17,33-39:2,41-42:14,49-50:12,51-58:3,59:16,60-69:19,70-74:1,75-110:18')
	rem(80,90,'5-19:17,30-32:17,33-39:2,41-42:14,49-50:12,51-58:3,59:16,60-69:19,70-74:1,75-79:18,90-110:18')
	rem(39,60,'5-19:17,30-32:17,33-38:2,60-69:19,70-74:1,75-79:18,90-110:18')
	rem(2,5,'5-19:17,30-32:17,33-38:2,60-69:19,70-74:1,75-79:18,90-110:18')
	rem(2,6,'6-19:17,30-32:17,33-38:2,60-69:19,70-74:1,75-79:18,90-110:18')
	rem(111,115,'6-19:17,30-32:17,33-38:2,60-69:19,70-74:1,75-79:18,90-110:18')
	rem(110,115,'6-19:17,30-32:17,33-38:2,60-69:19,70-74:1,75-79:18,90-109:18')
	rem(18,32,'6-17:17,32:17,33-38:2,60-69:19,70-74:1,75-79:18,90-109:18')
	rem(29,31,'6-17:17,32:17,33-38:2,60-69:19,70-74:1,75-79:18,90-109:18')

	a = Range()
	add(0,1,1,70,80,'70-79:1')
	add(0,2,1,30,40,'30-39:2,70-79:1')
	add(0,3,1,50,60,'30-39:2,50-59:3,70-79:1')
	add(0,None,1,11,19,'11-18,30-39:2,50-59:3,70-79:1')
	add(0,5,0,11,19,'11-18,30-39:2,50-59:3,70-79:1')
	add(0,6,1,10,19,'10:6,11-18,30-39:2,50-59:3,70-79:1')
	add(0,7,1,10,20,'10:6,11-18,19:7,30-39:2,50-59:3,70-79:1')
	add(0,8,0,10,19,'10:6,11-18,19:7,30-39:2,50-59:3,70-79:1')
	add(0,9,0,11,20,'10:6,11-18,19:7,30-39:2,50-59:3,70-79:1')
	add(0,10,1,90,100,'10:6,11-18,19:7,30-39:2,50-59:3,70-79:1,90-99:10')
	add(0,11,1,9,10,'9:11,10:6,11-18,19:7,30-39:2,50-59:3,70-79:1,90-99:10')
	add(0,12,1,49,51,'9:11,10:6,11-18,19:7,30-39:2,49:12,50-59:3,70-79:1,90-99:10')
	add(0,None,1,87,89,'9:11,10:6,11-18,19:7,30-39:2,49:12,50-59:3,70-79:1,87-88,90-99:10')
	add(0,14,1,41,43,'9:11,10:6,11-18,19:7,30-39:2,41-42:14,49:12,50-59:3,70-79:1,87-88,90-99:10')
	add(0,15,1,20,22,'9:11,10:6,11-18,19:7,20-21:15,30-39:2,41-42:14,49:12,50-59:3,70-79:1,87-88,90-99:10')
	add(0,16,1,59,61,'9:11,10:6,11-18,19:7,20-21:15,30-39:2,41-42:14,49:12,50-59:3,60:16,70-79:1,87-88,90-99:10')
	add(0,17,1,5,33,'5-8:17,9:11,10:6,11-18,19:7,20-21:15,22-29:17,30-39:2,41-42:14,49:12,50-59:3,60:16,70-79:1,87-88,90-99:10')
	add(0,18,1,75,111,'5-8:17,9:11,10:6,11-18,19:7,20-21:15,22-29:17,30-39:2,41-42:14,49:12,50-59:3,60:16,70-79:1,80-86:18,87-88,89:18,90-99:10,100-110:18')
	add(0,18,1,60,70,'5-8:17,9:11,10:6,11-18,19:7,20-21:15,22-29:17,30-39:2,41-42:14,49:12,50-59:3,60:16,61-69:18,70-79:1,80-86:18,87-88,89:18,90-99:10,100-110:18')
	rem(20,30,'5-8:17,9:11,10:6,11-18,19:7,30-39:2,41-42:14,49:12,50-59:3,60:16,61-69:18,70-79:1,80-86:18,87-88,89:18,90-99:10,100-110:18')
	rem(80,90,'5-8:17,9:11,10:6,11-18,19:7,30-39:2,41-42:14,49:12,50-59:3,60:16,61-69:18,70-79:1,90-99:10,100-110:18')
	rem(39,60,'5-8:17,9:11,10:6,11-18,19:7,30-38:2,60:16,61-69:18,70-79:1,90-99:10,100-110:18')
	rem(2,5,'5-8:17,9:11,10:6,11-18,19:7,30-38:2,60:16,61-69:18,70-79:1,90-99:10,100-110:18')
	rem(2,6,'6-8:17,9:11,10:6,11-18,19:7,30-38:2,60:16,61-69:18,70-79:1,90-99:10,100-110:18')
	rem(111,115,'6-8:17,9:11,10:6,11-18,19:7,30-38:2,60:16,61-69:18,70-79:1,90-99:10,100-110:18')
	rem(110,115,'6-8:17,9:11,10:6,11-18,19:7,30-38:2,60:16,61-69:18,70-79:1,90-99:10,100-109:18')
	rem(18,32,'6-8:17,9:11,10:6,11-17,32-38:2,60:16,61-69:18,70-79:1,90-99:10,100-109:18')
	rem(29,31,'6-8:17,9:11,10:6,11-17,32-38:2,60:16,61-69:18,70-79:1,90-99:10,100-109:18')


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

