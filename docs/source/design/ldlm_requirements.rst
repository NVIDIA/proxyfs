=================================================
ProxyFS Local and Distributed Lock Manager (LDLM)
=================================================

This document covers how ProxyFS does locking between threads on the local node as well as locking between nodes with the Local and Distributed Lock Manager (LDLM)

Requirements
============

The ProxyFS architecture assumes that the fs package implements locking between different threads doing operations regardless if the threads are on the same node or different nodes in the cluster.

A basic example is two threads each trying to create a new file in the same directory.  Serialization is required so that in the end the directory block stored in a log segment is updated correctly.

Therfore, we have these requirements:

1.  There needs to be a way to provide exclusive access to an inode i.e. adding an entry to a directory.
2.  There needs to be a way to provide shared access to an inode i.e. different threads reading the the contents of a directory.
3.  There needs to be a way for a remote node to request that the local node release the lock for an inode i.e. a thread wants to exclusively update the inode.
4.  Performance is important - therefore the local node should lazyily release the lock.  This means that even though a node is done with a lock it should not tell the lock master it is done with the lock until the lock master requests it.  This allows the local node to use the lock in another thread without needing to request the lock from the lock master.
5.  We want to scale out the lock master to as many of the proxy nodes as possible.

Basic Architecture
==================

There are two components to LDLM:

* Local Lock Manager (LLM) - which provides serialization amoungst threads in the local node
* Distributed Lock Manager (DLM) - which provides locks across the nodes in a cluster.

We will go into more detail below.

LDLM provides this API::

    // Lock the inode based on iLockState
    ldlm.GrantLock(inode inode.InodeNumber, lockTyp lLockState)

    // Release the lock on the inode to LLM.  LLM internally will decide whether or not to
    // release the lock to DLM if there is a pending callback for a release.
    ldlm.ReleaseLock(inode.InodeNumber)

The possible values for *iLockState* for granting a lock are::

    Shared - multiple threads could be accessing the inode
    Excl - only one thread can access the inode

Example code using this API is::

    ldlm.GrantLock(1, ldlm.EXCL)
    
    <change contents of inode 1>
    
    ldlm.ReleaseLock(1)
    
Internally, GrantLock() will check if the local node already has the lock and if it is no longer being used by another thread.  If the lock is free then it will grant the lock and return.  If it is not free, then GrantLock() will block until the lock is free at which time it can be granted.

Internally, ReleaseLock() will release the lock and grant it to another thread waiting for the lock or mark it as freed.   The local node will not release the lock to the DLM lock master unless it has been requested too.

Terms
-----

**DLM lock master** - Component of DLM. Node which maintains the state of an individual lock (Granted shared or exclusive, etc), the list of clients wanting the lock, etc.  Internally to LDLM there is a consistent hash used to find the lock master for a given inode amoungst the Proxy nodes.

**DLM** - Distributed Lock Manager - responsible for handling locks across a set of nodes and handing them out to LLM.

**LLM** - Local Lock Manager - responsible for getting lock from DLM and then sharing lock across multiple threads.


Implementing LDLM Locks in fs Package
=====================================

We believe that adding LDLM locks to the *fs* package will expose bugs other than just with locking.  Therefore, in order to reduce the implementation and stabilty time of the code base we are going to take an approach which attempts to uncover bugs with the least amount of variables as possible.  The approach is:

1. This testing will be done using a file system mounted via Samba
2. Add locking for *fs.CreateFile()* and *fs.Readdir()* and test one shell script creating files while another shell script is doing a *ls*.   The ls thread will make sure the files in the listing increase with a definite pattern in order i.e. file1, file2, etc with no gaps.
3. While running this test we will use the **Go Race Detector** to see if we are accessing variables without a lock.
4. Once 2 threads works successfully, we will add more threads and fix any bugs exposed.
5. Once 10 threads creating and doing ls of a directory works, we will move on to *fs.Mkdir()* and *fs.ReaddirPlus()*.
6. We will continue this strategy while slowly adding new file sytem primitives to the mix until all file system primitives support locks

The result of this is locking in the fs package as well as a set of unit tests targeted at locking.



Future Work - Roadmap
=====================

1. During multinode- add membership call back when node membership changes and we have to remaster the locks.
2. Add statistics for performance testing
3. Add error handling to LDLM
4. Intergrate with *etcd* for node membership (**memberd**), membership SQN in cluster messages and "catchup" for SQN gaps during membership changes.


Open Issues
-----------

1.  We are going to uncover other issues outside of locking so we need to be prepared for that.
2.  Rename source directory from dlm to ldlm
3.  Add more comments, Go Doc and cleanup code
4. Add error handling


