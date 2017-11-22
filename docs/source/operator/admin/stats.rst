===========================
Stats to capture in ProxyFS
===========================

This document captures the statistics we wish to capture in ProxyFS. The presumption is that ProxyFS will adopt the "statsd" convention and,
thus, directly benefit from the statistics capturing and reporting infrastructure of OpenStack Swift (e.g. SwiftStack Controller).

High level goals
================

1. Report rates for popular file system operations (e.g. read, write, create, delete)
2. Provide a breakdown of read and write sizes
3. Reveal latency and bandwidth for reads and writes
4. Expose sequential access versus random access for both reads and writes
5. Quantify overwrites versus non-overlapping writes

Instrumentation technique
-------------------------

Statsd defines a simple protocol that enables:

1. Typically UDP (for connectionless, largely non-blocking) versus TCP (connected, buffered, potentially blocking)
2. Simple ASCII protocol
3. Dotted metric notation (e.g. "proxyfs.write.latency")
4. Sample rate data reduction (stats reporting will scale up to approximate the fraction of stats actually sent)
5. Support for both accumulators (e.g. we got a 4KB write) as well as event reporting (e.g. this read took 10ms)

The format follows this simple form::

    <metric name>:<value>|<type>[|@<sample rate>]

where::

    <metric name> is of the suggested form source.category[.specific]+ (i.e. with one or more dot-deliniated "specific" parts)
    <value> is a floating point number
    <type> is one of:
        "g"  - Gauges: the client computes the total to currently present
        "c"  - Counters: the client says "increment/decrement by this much"... optionally scaled by <sample rate>
        "ms" - Timers: the client says "this event took this many milliseconds"
        "h"  - Histograms: like Timers but provides history over time of the values
        "m"  - Meters: the server computes the rate of this event being reported
    <sample rate> is a floating point number in the range (0.0, 1.0] (e.g. if 0.1, client logs only one-tenth of the events & server will multiply value by 10.0)

While the "spec" says individual stats are delineated by newlines ('\n'), it is ambiguous as to if the last one needs
a trailing newline. Swift also only currently supports UDP and only one stat update per UDP Packet. Swift also chose
to not add that trailing newline.

Where to instrument
-------------------

It would be ideal to identify one package in ProxyFS to be instrumented. Many of the events would be easily captured by either package fs
or package inode, paths that are common to all operations in the above goal set. Alas, neither is able to capture all conceivable events.

If we instrument at multiple layers (packages), care should be taken to avoid double counting.

It is proposed that initial instrumentation be implemented at only the package inode layer. Where insufficient, this will provide a template
for other packages to implement their unique instrumentation as well at a later time.

Proposed list of metrics (additional suggestions welcome)
---------------------------------------------------------

============================================================================= ======= ==========================================================
Metric                                                                         Type    Description                                              
----------------------------------------------------------------------------- ------- ----------------------------------------------------------
proxyfs.log-segment.create.operations                                            c     Log segments created                                     
proxyfs.garbage-collection.operations                                            c     Garbage collection operations                            
proxyfs.garbage-collection.log-segment.delete.operations                         c     Garbage collection log segments deleted                  
proxyfs.directory.create.operations                                              c     Directories created                                      
proxyfs.directory.destroy.operations                                             c     Directories destroyed                                    
proxyfs.directory.read.operations                                                c     Directory read operations                                
proxyfs.directory.read.entries                                                   c     Directory reads in entries                               
proxyfs.directory.read.bytes                                                     c     Directory reads in bytes                                 
proxyfs.file.create.operations                                                   c     Files created                                            
proxyfs.file.destroy.operations                                                  c     Files destroyed                                          
proxyfs.file.read.operations                                                     c     File read operations                                     
proxyfs.file.read.operations.size-up-to-4KB                                      c     File read operations of size in [ 0KB,  4KB]             
proxyfs.file.read.operations.size-up-to-8KB                                      c     File read operations of size in ( 4KB,  8KB]             
proxyfs.file.read.operations.size-up-to-16KB                                     c     File read operations of size in ( 8KB, 16KB]             
proxyfs.file.read.operations.size-up-to-32KB                                     c     File read operations of size in (16KB, 32KB]             
proxyfs.file.read.operations.size-up-to-64KB                                     c     File read operations of size in (32KB, 64KB]             
proxyfs.file.read.operations.size-over-64KB                                      c     File read operations of size in (64KB, ... ]             
proxyfs.file.read.bytes                                                          c     File reads in bytes                                      
proxyfs.file.readcache.hit.operations                                            c     File read step operations that hit  the cache            
proxyfs.file.readcache.miss.operations                                           c     File read step operations that miss the cache            
proxyfs.file.write.operations                                                    c     File write operations                                    
proxyfs.file.write.operations.size-up-to-4KB                                     c     File write operations of size in [ 0KB,  4KB]            
proxyfs.file.write.operations.size-up-to-8KB                                     c     File write operations of size in ( 4KB,  8KB]            
proxyfs.file.write.operations.size-up-to-16KB                                    c     File write operations of size in ( 8KB, 16KB]            
proxyfs.file.write.operations.size-up-to-32KB                                    c     File write operations of size in (16KB, 32KB]            
proxyfs.file.write.operations.size-up-to-64KB                                    c     File write operations of size in (32KB, 64KB]            
proxyfs.file.write.operations.size-over-64KB                                     c     File write operations of size in (64KB, ... ]            
proxyfs.file.write.bytes                                                         c     File writes in bytes                                     
proxyfs.file.write.bytes.appended                                                c     File writes appended bytes                               
proxyfs.file.write.bytes.overwritten                                             c     File writes overwritten bytes                            
proxyfs.file.flush.operations                                                    c     File flush operations                                    
proxyfs.file.readplan.operations                                                 c     File readplan operations                                 
proxyfs.file.readplan.operations.size-up-to-4KB                                  c     File readplan operations of size in [ 0KB,  4KB]         
proxyfs.file.readplan.operations.size-up-to-8KB                                  c     File readplan operations of size in ( 4KB,  8KB]         
proxyfs.file.readplan.operations.size-up-to-16KB                                 c     File readplan operations of size in ( 8KB, 16KB]         
proxyfs.file.readplan.operations.size-up-to-32KB                                 c     File readplan operations of size in (16KB, 32KB]         
proxyfs.file.readplan.operations.size-up-to-64KB                                 c     File readplan operations of size in (32KB, 64KB]         
proxyfs.file.readplan.operations.size-over-64KB                                  c     File readplan operations of size in (64KB, ... ]         
proxyfs.file.readplan.bytes                                                      c     File readplans in bytes                                  
proxyfs.file.wrote.operations                                                    c     File wrote operations                                    
proxyfs.file.wrote.operations.size-up-to-4KB                                     c     File wrote operations of size in [ 0KB,  4KB]            
proxyfs.file.wrote.operations.size-up-to-8KB                                     c     File wrote operations of size in ( 4KB,  8KB]            
proxyfs.file.wrote.operations.size-up-to-16KB                                    c     File wrote operations of size in ( 8KB, 16KB]            
proxyfs.file.wrote.operations.size-up-to-32KB                                    c     File wrote operations of size in (16KB, 32KB]            
proxyfs.file.wrote.operations.size-up-to-64KB                                    c     File wrote operations of size in (32KB, 64KB]            
proxyfs.file.wrote.operations.size-over-64KB                                     c     File wrote operations of size in (64KB, ... ]            
proxyfs.file.wrote.bytes                                                         c     File wrotes in bytes                                     
proxyfs.symlink.create.operations                                                c     Symlinks created                                         
proxyfs.symlink.destroy.operations                                               c     Symlinks destroyed                                       
proxyfs.symlink.read.operations                                                  c     Symlink read operations                                  
proxyfs.swiftclient.chunked-connections-create-operations                        c     Swift Chunked     Connection Create operations           
proxyfs.swiftclient.chunked-connections-reuse-operations                         c     Swift Chunked     Connection Reuse  operations           
proxyfs.swiftclient.non-chunked-connections-create-operations                    c     Swift Non-Chunked Connection Create operations           
proxyfs.swiftclient.non-chunked-connections-reuse-operations                     c     Swift Non-Chunked Connection Reuse  operations           
proxyfs.swiftclient.account-delete.operations                                    c     Swift Account   DELETE operations                        
proxyfs.swiftclient.account-get.operations                                       c     Swift Account   GET    operations                        
proxyfs.swiftclient.account-head.operations                                      c     Swift Account   HEAD   operations                        
proxyfs.swiftclient.account-put.operations                                       c     Swift Account   PUT    operations                        
proxyfs.swiftclient.container-delete.operations                                  c     Swift Container DELETE operations                        
proxyfs.swiftclient.container-get.operations                                     c     Swift Container GET    operations                        
proxyfs.swiftclient.container-head.operations                                    c     Swift Container HEAD   operations                        
proxyfs.swiftclient.container-put.operations                                     c     Swift Container PUT    operations                        
proxyfs.swiftclient.object-content-length.operations                             c     Swift Object fetch Content-Length operations             
proxyfs.swiftclient.object-copy.operations                                       c     Swift Object Copy      operations                        
proxyfs.swiftclient.object-delete.operations                                     c     Swift Object    DELETE operations                        
proxyfs.swiftclient.object-put-context.fetch.operations                          c     Swift Object Fetch Chunked Put Context operations        
proxyfs.swiftclient.object-put-context.bytes-put.operations                      c     Swift Object BytesPut operations                         
proxyfs.swiftclient.object-put-context.close.operations                          c     Swift Object Close     operations                        
proxyfs.swiftclient.object-put-context.read.operations                           c     Swift Object Read      operations                        
proxyfs.swiftclient.object-put-context.read.operations.size-up-to-4KB            c     Swift Object Read      operations of size in ( 0KB,  4KB]
proxyfs.swiftclient.object-put-context.read.operations.size-up-to-8KB            c     Swift Object Read      operations of size in ( 4KB,  8KB]
proxyfs.swiftclient.object-put-context.read.operations.size-up-to-16KB           c     Swift Object Read      operations of size in ( 8KB, 16KB]
proxyfs.swiftclient.object-put-context.read.operations.size-up-to-32KB           c     Swift Object Read      operations of size in (16KB, 32KB]
proxyfs.swiftclient.object-put-context.read.operations.size-up-to-64KB           c     Swift Object Read      operations of size in (32KB, 64KB]
proxyfs.swiftclient.object-put-context.read.operations.size-over-64KB            c     Swift Object Read      operations of size in (64KB, ... ]
proxyfs.swiftclient.object-put-context.read.bytes                                c     Swift Object Read      operations in bytes               
proxyfs.swiftclient.object-put-context.retry.operations                          c     Swift Object Retry     operations                        
proxyfs.swiftclient.object-put-context.send-chunk.operations                     c     Swift Object SendChunk operations                        
proxyfs.swiftclient.object-put-context.send-chunk.operations.size-up-to-4KB      c     Swift Object SendChunk operations of size in ( 0KB,  4KB]
proxyfs.swiftclient.object-put-context.send-chunk.operations.size-up-to-8KB      c     Swift Object SendChunk operations of size in ( 4KB,  8KB]
proxyfs.swiftclient.object-put-context.send-chunk.operations.size-up-to-16KB     c     Swift Object SendChunk operations of size in ( 8KB, 16KB]
proxyfs.swiftclient.object-put-context.send-chunk.operations.size-up-to-32KB     c     Swift Object SendChunk operations of size in (16KB, 32KB]
proxyfs.swiftclient.object-put-context.send-chunk.operations.size-up-to-64KB     c     Swift Object SendChunk operations of size in (32KB, 64KB]
proxyfs.swiftclient.object-put-context.send-chunk.operations.size-over-64KB      c     Swift Object SendChunk operations of size in (64KB, ... ]
proxyfs.swiftclient.object-put-context.send-chunk.bytes                          c     Swift Object SendChunk operations in bytes               
proxyfs.swiftclient.object-get.operations                                        c     Swift Object    GET    operations                        
proxyfs.swiftclient.object-get.operations.size-up-to-4KB                         c     Swift Object    GET    operations of size [ 0KB,  4KB]   
proxyfs.swiftclient.object-get.operations.size-up-to-8KB                         c     Swift Object    GET    operations of size ( 4KB,  8KB]   
proxyfs.swiftclient.object-get.operations.size-up-to-16KB                        c     Swift Object    GET    operations of size ( 8KB, 16KB]   
proxyfs.swiftclient.object-get.operations.size-up-to-32KB                        c     Swift Object    GET    operations of size (16KB, 32KB]   
proxyfs.swiftclient.object-get.operations.size-up-to-64KB                        c     Swift Object    GET    operations of size (32KB, 64KB]   
proxyfs.swiftclient.object-get.operations.size-over-64KB                         c     Swift Object    GET    operations of size (64KB, ... ]   
proxyfs.swiftclient.object-load.operations                                       c     Swift Object   "LOAD"  operations                        
proxyfs.swiftclient.object-load.operations.size-up-to-4KB                        c     Swift Object   "LOAD"  operations of size [ 0KB,  4KB]   
proxyfs.swiftclient.object-load.operations.size-up-to-8KB                        c     Swift Object   "LOAD"  operations of size ( 4KB,  8KB]   
proxyfs.swiftclient.object-load.operations.size-up-to-16KB                       c     Swift Object   "LOAD"  operations of size ( 8KB, 16KB]   
proxyfs.swiftclient.object-load.operations.size-up-to-32KB                       c     Swift Object   "LOAD"  operations of size (16KB, 32KB]   
proxyfs.swiftclient.object-load.operations.size-up-to-64KB                       c     Swift Object   "LOAD"  operations of size (32KB, 64KB]   
proxyfs.swiftclient.object-load.operations.size-over-64KB                        c     Swift Object   "LOAD"  operations of size (64KB, ... ]   
proxyfs.swiftclient.object-tail.operations                                       c     Swift Object   "TAIL"  operations                        
proxyfs.swiftclient.object-tail.bytes                                            c     Swift Object   "TAIL"  operations in bytes               
proxyfs.logging.trace.enter.operations                                           c     Logging Trace Enter Calls                                
proxyfs.logging.trace.exit.operations                                            c     Logging Trace Exit  Calls                                
proxyfs.logging.level.debug.operations                                           c     Logging Debug Level Operations                           
proxyfs.logging.level.debug.bytes                                                c     Logging Debug Level Bytes                                
proxyfs.logging.level.error.operations                                           c     Logging Error Level Operations                           
proxyfs.logging.level.error.bytes                                                c     Logging Error Level Bytes                                
proxyfs.logging.level.fatal.operations                                           c     Logging Fatal Level Operations                           
proxyfs.logging.level.fatal.bytes                                                c     Logging Fatal Level Bytes                                
proxyfs.logging.level.info.operations                                            c     Logging Info  Level Operations                           
proxyfs.logging.level.info.bytes                                                 c     Logging Info  Level Bytes                                
proxyfs.logging.level.panic.operations                                           c     Logging Panic Level Operations                           
proxyfs.logging.level.panic.bytes                                                c     Logging Panic Level Bytes                                
proxyfs.logging.level.trace.operations                                           c     Logging Trace Level Operations                           
proxyfs.logging.level.trace.bytes                                                c     Logging Trace Level Bytes                                
proxyfs.logging.level.warn.operations                                            c     Logging Warn  Level Operations                           
proxyfs.logging.level.warn.bytes                                                 c     Logging Warn  Level Bytes                                
============================================================================= ======= ==========================================================
