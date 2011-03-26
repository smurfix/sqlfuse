========
sqlmount
========

This program mounts a file hierarchy, stored in a SQL database,
onto a directory (via FUSE).

The actual file contents are not stored in the database.
That would be much too slow. Instead, they're stored in a
separate file hierarchy, indexed by inode.

If files are created or changed, this is recorded in the database.
You can use another sqlmount process to use these change records
to mirror files.

---------
Rationale
---------

For my own use, I want a file system with a bunch of properties
not commonly found in "standard" distributed file systems.

I want a file system that's replicated across multiple systems,
some of which are behind slow WAN links. Yet local access needs
to be (reasonably) fast everywhere.

I want a file system that's redundant and that can be reconfigured
easily, so that I can add a subtree with my banking records and
decide to have a few more copies stored someplace safe.

I don't need safe concurrent write accesses, but it should be
possible to arrange for that.

Local file caches should be possible but not required.

I like to store file metadata in a database instead of modifying
the original files. That's important for stuff like MP3 tracks.

---------
Structure
---------

All file metadata are stored in the database.
You find your basic inodes there, as well as a directory tree.

Within a database, you may have multiple "root" directories.
Each of these needs at least one storage node, which specifies
where the actual file contents are stored. You may run at most
one sqlmount process per node. Nodes may use shared file storage.

If you have more than one node per root, you should also
specify copy methods. These allow new files to be transferred
from A to B automatically, either "soon" or on demand.
(Of course, updates and deletes are also synchronized.)

-----
Usage
-----

	sqlmount --help
	sqlfutil --help

----
Bugs
----

Directory mtimes are not updated.

A basic file system works, but nothing else is implemented;
the SQL table structure _will_ change.

-----
Ideas
-----

Mostly-read-only access: return symlinks to the storage for all
files, or for all "old" files (define 'old').

Overlay file reading. This enables you to e.g.  store unmodified
MP3 files via sqlmount, except that the ID3v2 metadata are pulled
transparently from (a local mirror of) the MusicBrainz database.

Files might be stored in archives. People who like P2P file sharing
can thus keep the original file, and still access the individual parts.

----
TODO
----

Index files by content (file SHA1), store identical files only once.
This means copy-on-write, probably intelligently (i.e. copy only parts).

If we can copy parts, creating snapshots becomes possible.

Add files to the storage without going through the file system.

Protect against SQL server restarts and timeouts.

Implement file locking. This requires some sort of broadcast feature.

Implement a ssh file-copying backend, probably with a channel for the PB
connection.

Implement node-specific database storage (sqlite; directly push inode
updates when connected; remember to push inode updates to other nodes;
re-sync data when nodes connect).

