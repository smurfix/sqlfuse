#!/usr/bin/env python

"""Setup script for smurf's SQL-Fuse stuff"""

from distutils.core import setup

description = "A distributed SQL-based FUSE file system"
long_description = \
"""
This program mounts a SQL-based file system via FUSE.

All data are replicated (or cached).

"""

from sqlfuse import DBVERSION

setup (name = "sqlfuse",
       version = DBVERSION,
       description = description,
       long_description = long_description,
       author = "Matthias Urlichs",
       author_email = "smurf@smurf.noris.de",
       url = "http://netz.smurf.noris.de/cgi/gitweb?p=sqlfuse.git",
       license = 'Python',
       platforms = ['POSIX'],
       packages = ['sqlfuse'],
	   scripts = ['sqlmount', 'sqlfutil'],
      )
