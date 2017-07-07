# ProxyFS Bimodal Support of Object Storage API v1

This document covers how ProxyFS supports the Swift Object Storage API listed [at](http://developer.openstack.org/api-ref-objectstorage-v1.html)

# What is bimodal support?

Bimodal support means that a file can be accessed by the object store and then accessed by the file system.

Conversely, the file can be accessed by the file system and then accessed by the object store.


# How does a file system path look compared to an object storage path?

If you store cat.jpg in the file system you would see it at this location:
```
    smb://domain.name/export/vcontainer/vdir1/vdir2/cat.jpg
```
Where Swift Account named "vaccount" is exported via SMB as "export".

If you wanted to see the same cat.jpg as an object you would find it here:
```    
http://domain.name/v1/vaccount/vdir1/vdir2/cat.jpg
```

Where domain.name is the Domain Name or IP Address of a Swift Proxy node (possibly a "VIP" - or virtual IP serviced by a set of Swift Proxy Nodes and possibly load balanced by having multiple such VIPs for a domain.name A record).

# How does ProxyFS interpret the path sent by middleware?

Middleware (MW) will send a path as <I>/v1/account/container/object</I>

However, <I>object</I> can also contain a file system path.  For example, this is a valid path:

```    /v1/account1/container1/vdir1/vdir2/file1
```

and ```vdir1/vdir2/cat.jpg``` is the object.


#Discoverability
| Verb | Summary| Status|
|------|:-------|:------|
|GET /info|Lists the activated capabilities for this version of the OpenStack Object Storage API.|ProxyFS middleware will forward to next module in pipeline|

#Endpoints
| Verb | Summary| Status|
|------|:-------|:------|
|GET /v1/endpoints|Lists endpoints for an object, account, or container.|ProxyFS middleware will forward to next module in pipeline|

#Accounts
| Verb | Summary| Status|Algorithm|Open Issues|
|------|:-------|:------|:--------|:----------|
|GET /v1/{account}|Show account details and list of containers sorted by name in forward or reverse order.|Deferred but may need| |Must provide prefix order which means<ul><li>The provided prefix is actually a dirpath. </li><li>The provided prefix selects perhaps a set of dir entries some or all of which are sub dirs but some might be objects/files.</li>|
|POST /v1/{account}|Creates, updates, or deletes account metadata.|Current implementation returns error for POST on an account.|Refer to Swift spec for rules on how metadata is updated via POST.|<ol><li>May be needed for implementing billing for some installations. May wire to do statfs for accounts?</li><li>May want Swift metadata (getable) but not setable</li><li>Do we want to allow metadata to be modified for an account?</li></ol>|
|HEAD /v1/{account}|Shows metadata for an account.|Deferred but may need|||

#Containers
| Verb | Summary| Status|Algorithm|Open Issues|
|------|:-------|:------|:--------|:----------|
|GET /v1/{account}/{container}|Shows details for a container and lists objects, sorted by name, in the container.  Must show in forward or reverse order.|Basic implmentation. Some of what is not implemented but needed is prefix (i.e. *cat.jpg).  Will defer suffix and content type|<ol><li>MW sends path to ProxyFS</li><li>ProxyFS does "ls -R" lexigraphically by name in either forward or reverse order and sends results to MW</li><li>MW sends back "last file or directory seen" as a marker to get next batch of listing.</li> </ol>|<ol><li>// is valid in Swift but not in file system</li><li>How implement prefix, suffix and content type?</li><li>Must provide prefix order which means<ul><li>The provided prefix is actually a dirpath.</li><li>The provided prefix selects perhaps a set of dir entries some or all of which are sub dirs but some might be objects/files.</li></ul></li><li>Output is not sorted.</li><li>The contents of the subdirectories is not retrieved.</li><li>What if inode is a symlink?</li></ol>|
|PUT /v1/{account}/{container}|Creates a container.  <B>NOTE:</B> Implicit create of account is not possible since could not create it bimodally or export if we did.|Basic implementation without handling of metadata|<ol><li>mkdir of the container</li><li>Possible to have metadata</li><li>Put to existing container just updates metadata</li></ol>|<ol><li>Metadata not implemented</li><li>Subdirectories are not supported.</li><li>How overwrite existing file?</li><li>How handle symlinks?</li></ol>|
|DELETE /v1/{account}/{container}|Deletes an empty container.|Implemented but due to a bug the container is not removed.|rmdir - Remove container if empty| <ol><li>Returns errno 2 even if container empty.</li><li>What error returned if container not empty?</li></ol>|
|POST /v1/{account}/{container}|Creates, updates, or deletes custom metadata for a container.|Implemented|<ol><li>MW reads HTTP headers from file system</li><li>MW merges changes, passes original metadata, new metadata</li><li>RPC code attempts to update, update fails if original metadata does not match on disk and then returns error.</li><li>MW keeps retrying until no changes to existing metadata when doing update</li></ol>||
|HEAD /v1/{account}/{container}|Shows container metadata, including the number of objects and the total bytes of all objects stored in the container.|Deferred|TBD - Algorithm|<ol><li>Not implemented</li><li>How show total bytes?</li></ol>| 

#Objects
| Verb | Summary| Status|Algorithm|Open Issues|
|------|:-------|:------|:--------|:----------|
|GET /v1/{account}/{container}/{object}|Downloads the object content and gets the object metadata.|Basic implementation|See docs/bimodal-get.txt for details but summary is:<ol><li>MW sends path to ProxyFS</li><li>ProxyFS checks if object is directory and if so returns a 0 length response.</li><li>ProxyFS checks if object is file and if so returns ReadPlan and metadata for file.</li><li>If MW sees reply with ReadPlan, MW does GETs from SwiftBE and when done sends release lease on ReadPlan to ProxyFS.</li></ol>|<ol><li>What error returned if object not there?</li><li>ReadPlan will have:</li><ul><li>"literal" data (for when we've done the LogSegment read(s) for them already)</li><li>"zero-fill" for logical byte ranges that are unwritten</li><li>"reference" case where we are telling MW the {LogSegment vPath, offset, and length} tuple</li></ul><li>Probably also need "renew lease" API</li><li>GetReadPlanDone() is not implemented.  We don't yet have garbage collection so this is not an issue yet.</li></ol>|
|PUT /v1/{account}/{container}/{object}|Creates an object with data content and metadata, or replaces an existing object with data content and metadata.|Basic implementation|See docs/bimodal-put-newfile.txt for details but HIGH LEVEL SUMMARY is:<ol><li>In FS code, do lookup of object and if it does not exist then create it and return.</li><li>In FS code, do lookup of object and if it does exist then create new one and delete old one.</li><li>If replace object then must also remove metadata.</li></ol>|<ol><li>What if existing object is not empty?  How handle? What if hard link to file? </li><li>Assume that "existing" means exactly the same object path.</li><li>No support for replacement of a file.</li><li>An optimization is that PUT should just send the data and avoid the ProvisionObject() step if we have ContentValue.</li></ol>|
|COPY /v1/{account}/{container}/{object}|Copies an object to another object in the object store.|ProxyFS middleware will take care of and no work required in ProxyFS|||
|DELETE /v1/{account}/{container}/{object}|Permanently deletes an object from the object store.|Implemented|Remove file.  If a directory, only remove if empty.|<ol><li>If passed /dir1/dir2/f1 will only remove file.</li><li>If passed /dir1/dir2 will only remove dir2 if directory empty.</li><li>What do if file is a symlink?</li></ol>|
|HEAD /v1/{account}/{container}/{object}|Shows object metadata.|Not implemented but need|RPC does getattr on leaf of object||
|POST /v1/{account}/{container}/{object}|Creates or updates object metadata.|Implemented|<ol><li>MW reads HTTP headers from file system</li><li>MW merges changes, passes original metadata, new metadata</li><li>RPC code attempts to update, update fails if original metadata does not match on disk and then returns error.</li><li>MW keeps retrying until no changes to existing metadata when doing update</li></ol>||

#Unit Test for Bimodal

NOTE: There are now unit tests in the source located at jrpcfs/middleware_test.go

1. Create container "my_container" using account "AUTH_test":
 <pre>
   # cdpfs
   # cd jrpcfs/gotest
   # go build client.go
   # ./client -a AUTH_test -c my_container -r
   </pre>
2. Read contents of container (should only see "." and "..")
 <pre>
   # ./client -a AUTH_test -c my_container -l
   </pre>
3. Create object (PUT) "my_object" in "my_container" by reading contents of file "putfile".
 <pre>
   # cp /etc/passwd putfile
   # ./client -a AUTH_test -c my_container -o my_object -f putfile -u
   </pre>
4. Verify that "my_object" now exists in container by reading the contents of the container again:
 <pre>
   # ./client -a AUTH_test -c my_container -l
   </pre>
5. GET contents of "my_object" and store in file named "copy_of_object:
 <pre>
   # ./client -a AUTH_test -c my_container -o my_object  -d -f copy_of_object
   </pre>
6. Make sure the files are the same:
 <pre>
   # diff putfile copy_of_object
 </pre>
7. Delete "my_object"
 <pre>
   # ./client -a AUTH_test -c my_container -o my_object -x
   </pre>
8. Verify that "my_object" no longer exists in container by reading the contents of the container again:
 <pre>
   # ./client -a AUTH_test -c my_container -l
   </pre>

#Open Issues
1.  Can't touch /mnt/some_account/foo.  How handle in Swift since can't see since not in container. Could it just be a PUT with some sort of IF header that meant it only worked if the object didn't previously exist?
3.  Only containers and objects go through middleware to ProxyFS?
5.  Need account for GET of account?
6.  What error codes need to be returned?
7.  Remove AuthUser from RPC
9.  Need lease or some similar concept for ReadPlan
11.  Anything special to worry about with symlink vs file.
12.  Work into document that MW will not send "*" or file globbing requests.
13.  Do we have any chunking requirements for PUT?
14.  How implement remove of entire path created by HTTP but leave path created by file system? Consider this case:
     1. mkdir /a/c/d1/d2
     2. put /a/c/d1/d2/d3/d4/cat.jpg (which will create inode for d3, create inode for d4 in d3, create inode  cat.jpg d3/d4/cat.jpg and finally insert d3 in d2/d3)
     3. delete /a/c/d1/d2/d3/d4/cat.jpg (which will delete cat.jpg, what do to remove d4, d3?)
15. Proposed solution to remove of file listed above is to add bit in inode attribute (alternate data stream).
     1. The bit is "WasUsedByFS"
     2. If inode created by ProxyFS operation then "WasUsedByFS=1"
     3. If inode created by HTTP PUT then "WasUsedByFS=0"
     4. If inode modified by ProxyFS then "WasUsedByFS=1"
     5. When do a HTTP DELETE delete all leaves with "WasUsedByFS=0" and all empty parents with "WasUsedByFS=0"
17. How handle hard links?  Consider this case:
     1. touch a
     2. ln b a
     3. PUT b - will this change the contents of a AND B or when we try this does it create a new file with a new inode?




