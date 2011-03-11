# -*- coding: utf-8 -*-

#    Copyright (C) 2010,2011  Matthias Urlichs <matthias@urlichs.de>
#
#    This program may be distributed under the terms of the GNU GPLv3.
#
## This file is formatted with tabs.
## Do NOT introduce leading spaces.

from __future__ import absolute_import

"""\
This module documents the server-to-server communications back-end.
"""

from zope.interface import implements,Interface

METHODS = "native".split()

class INodeClient(Interface):
	"""\
		I represent what a node can do to its remote side.

		(Almost) all my method names start with "do_".
		"""
	def init(node):
		"""Which node to control."""
	def connect():
		"""Try to connect to this server. Returns a success/fail Deferred."""
	def disconnect():
		"""Shutdown, or abort trying to connect."""

	def do_echo(data):
		"""Instruct the remote side to send the given data back."""
	


class INode(Interface):
	"""\
		I represent a node -- specifically, what a remote side can ask a
		node to do.
		"""
	
	def echo(data):
		"""Instruct the node side to send the given data back."""
	
	def server_connected(node_server):
		"""This server is new.
		The node will kill any old server connection."""

	def server_disconnected(node_server):
		"""This server is dead."""
		
	def client_connected(node_client):
		"""This client is active.
		Whether to kill old clients is not specified.

		This tells the node about the client for purposes of documentation
		and clean shutdown only; the node does not otherwise call the
		client.
		"""

	def client_disconnected(node_client):
		"""This client is no longer active."""
		

class INodeServer(Interface):
	"""\
		I represent the front-end which controls a node.

		My methods are protocol specific, but their name never starts with "do_".
		They are invoked from the remote side and call my node.
		"""
	#node = None

	def disconnect():
		"""Drop the connection to this server."""


class INodeServerFactory(Interface):
	"""The point other clients can connect to."""

	def init(fs):
		"""The SqlFuse instance I serve."""
	
	def connect():
		"""Start serving."""

	def disconnect():
		"""Shutdown: Die."""
	

