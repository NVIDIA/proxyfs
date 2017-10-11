# pick up standard settings
#
#python
#import os
#gdb.execute('source' + os.environ['SWIFT_DIR'] + '/qatt/scripts/gdbinit')
#end

# pick up go helpers
add-auto-load-safe-path /usr/local/go/src/runtime/runtime-gdb.py

# gdb debugging tips
#
# Some of this info came from:
#     http://www.bonsai.com/wiki/howtos/debugging/gdb/
#
# You can also define functions that add to what gdb can do.
# I didn't do any, but you can see some complex ones here:
#     http://web.mit.edu/bzbarsky/www/gdbinit
#
# Some tips on using gdb with STL is from this useful gdb tip site:
#     http://sourceware.org/gdb/wiki/STLSupport
#
# You can put the following commands in your ~/.gdbinit file
# to make gdb more tractable for debugging Picasso (with its
# many threads and fancy data structures).
#
# Any of these commands can also be entered from the gdb prompt
# to selectively enable or disable them
#
# (Note that `#' functions as a comment character only at the
# beginning of the line).
#

# don't print thread start/stop msgs
#
set print thread-events off

# pretty print structures 
# (one field per line, indented to show nesting)
#
set print pretty on

# enable the Printing of C++ virtual function tables
#
set print vtbl on

# Set printing of C++ static members.
#
set print static-members off

# Set printing of object's derived type based on vtable info.
#
set print object on

# Set demangling of C++/ObjC names in disassembly listings.
#
set print asm-demangle on

# You can disable the gdb pager with this command
# (or set the number of lines to page)
#
# set height 0
set pagination off

# Log all gdb output to file "gdb.out".
# It is overwritten each time gdb starts.
#
# Enable "set logging redirect on" to send output only to the file.
#
set logging file ~/gdb.out
set logging overwrite on
set logging on
# set logging redirect on

# Set mode for locking scheduler during execution.
# off  == no locking (threads may preempt at any time)
# on   == full locking (no thread except the current thread may run)
# step == scheduler locked during every single-step operation.
# 	In this mode, no other thread may run during a step command.
# 	Other threads may run while stepping over a function call ('next').
#
# This will generate a diagnostic if you don't specify a target file:
#     /home/charmer/.gdbinit:71: Error in sourced command file:
#     Target 'None' cannot support this command.
#
# set scheduler-locking step

# Capture backtraces from all threads:
#
# thread apply all bt

# Find the source line number of an assembly address
#
# info line * 0x2530526

# Run gdb using the "Text User Interface (TUI)"
# command line:  gdb -tui ...
# gdb prompt:  "C-x C-a"
# Documention on the TUI for gdb 7.2 can be found here:
#     http://www.ecoscentric.com/ecospro/doc/html/gnutools/share/doc/gdb/index.html 
# and on the TUI for gdb 7.4 here:
#     http://sources.redhat.com/gdb/download/onlinedocs/gdb.html 
# 
# Once in the TUI you may prefer "single key mode" enabled with "C-x s".
#
# Emacs also as a "gdb mode" that runs gdb as a sub-shell with emacs
# editing available.  It looks promising.
#
 
