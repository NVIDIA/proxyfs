// JSON RPC Server on top of FS package.
package jrpcfs

import (
	"container/list"
	"fmt"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/utils"
)

// RPC server handle
var srv *rpc.Server
var jrpcListener net.Listener

// Local server handle, used to track jrpc-related ops
var jserver *Server

func jsonRpcServerUp(ipAddr string, portString string) {
	var err error

	jserver = NewServer()

	srv = rpc.NewServer()
	err = srv.Register(jserver)
	if err != nil {
		logger.ErrorfWithError(err, "failed to register RPC handler")
		return
	}

	jrpcListener, err = net.Listen("tcp", net.JoinHostPort(ipAddr, portString))
	if err != nil {
		logger.ErrorfWithError(err, "net.Listen %s:%s failed", ipAddr, portString)
		return
	}

	globals.connLock.Lock()
	globals.listeners = append(globals.listeners, jrpcListener)
	globals.connLock.Unlock()

	//logger.Infof("Starting to listen on %s:%s", ipAddr, portString)
	globals.listenersWG.Add(1)
	go jrpcServerLoop()
}

func jrpcServerLoop() {
	for {
		conn, err := jrpcListener.Accept()
		if err != nil {
			if !globals.halting {
				logger.ErrorfWithError(err, "net.Accept failed for JRPC listener\n")
			}
			globals.listenersWG.Done()
			return
		}

		globals.connWG.Add(1)

		globals.connLock.Lock()
		elm := globals.connections.PushBack(conn)
		globals.connLock.Unlock()

		go func(myConn net.Conn, myElm *list.Element) {
			srv.ServeCodec(jsonrpc.NewServerCodec(myConn))
			globals.connLock.Lock()
			globals.connections.Remove(myElm)

			// There is a race condition where the connection could have been
			// closed in Down().  However, closing it twice is okay.
			myConn.Close()
			globals.connLock.Unlock()
			globals.connWG.Done()
		}(conn, elm)
	}
}

func jsonRpcServerDown() {
	DumpIfNecessary(jserver)
	stopServerProfiling(jserver)
}

// Enumeration of operations, used for stats-related things
type OpType int

// NOTE: When you add a new OpType here, be sure to add its string
//       at the appropriate spot in opTypeStrs below.
const (
	PingOp OpType = iota
	ReadOp
	WriteOp
	FlushOp
	LookupOp
	LookupPathOp
	GetStatOp
	GetStatPathOp
	GetXattrOp
	GetXattrPathOp
	ReaddirOp
	ReaddirByLocOp
	ReaddirPlusOp
	ReaddirPlusByLocOp
	InvalidOp // Keep this as the last entry!
)

var opTypeStrs = []string{
	"Ping",
	"Read",
	"Write",
	"Flush",
	"Lookup",
	"LookupPath",
	"GetStat",
	"GetStatPath",
	"GetXattr",
	"GetXattrPath",
	"Readdir",
	"ReaddirByLoc",
	"ReaddirPlus",
	"ReaddirPlusByLoc",
	"InvalidOp",
}

// Return operation type as a string
func (op OpType) String() string {
	return opTypeStrs[op]
}

// Get operation type from string
func strToOpType(opStr string) OpType {
	for opType, strVal := range opTypeStrs {
		if opStr == strVal {
			return OpType(opType)
		}
	}
	return InvalidOp
}

// Map of profilers
type opProfiles map[int]*utils.Profiler

// All of our RPC methods are called on/passed this Server struct.
type Server struct {
	opStatsLock           sync.Mutex
	maxSavedProfilesPerOp int
	allProfiles           map[OpType]opProfiles
	saveLock              sync.Mutex
	saveChannel           chan *utils.Profiler
	internalSaveChannel   chan *utils.Profiler
	saveProfilerProfiles  opProfiles
	dumpRunning           bool
	dumpLock              sync.Mutex
}

func NewOpProfiles() opProfiles {
	return make(opProfiles, maxSavedStatProfiles)
}

func AllocOpProfiles() (theOpProfiles map[OpType]opProfiles) {
	if !doProfiling {
		return theOpProfiles
	}

	theOpProfiles = make(map[OpType]opProfiles, int(InvalidOp))

	// Cheater range check on opTypeStrs versus number of OpTypes
	if len(opTypeStrs) != int(InvalidOp)+1 {
		// XXX TODO: Find a way to make this a compile-time check instead
		panic("len(opTypeStrs) != int(InvalidOp)+1!")
	}

	// Allocat profile space for each op type
	for i, _ := range opTypeStrs {
		theOpProfiles[OpType(i)] = NewOpProfiles()
	}

	return theOpProfiles
}

// Stats storage
var maxSavedStatProfiles int = 4000

// Threshold of when to trigger stats dump/free
var statsCleanupThreshold int = 100

// Size of each nonblocking channel
var saveChannelSize int = 1000

// Default values here are false
var loggedOutOfStatsRoom map[OpType]bool = make(map[OpType]bool)

func allocateMountID(mountHandle fs.MountHandle) (mountID uint64) {
	globals.mapsLock.Lock()
	globals.lastMountID++
	mountID = globals.lastMountID
	globals.mountIDMap[mountID] = mountHandle
	globals.mapsLock.Unlock()
	return
}

func lookupMountHandle(mountID uint64) (mountHandle fs.MountHandle, err error) {
	globals.mapsLock.Lock()
	mountHandle, ok := globals.mountIDMap[mountID]
	globals.mapsLock.Unlock()
	if ok {
		err = nil
	} else {
		err = fmt.Errorf("MountID %v not found in jrpcfs globals.mountIDMap", mountID)
		err = blunder.AddError(err, blunder.BadMountIDError)
	}
	return
}

func NewServer() *Server {
	s := Server{}

	// Set profiling-related variables
	if doProfiling {
		s.opStatsLock = sync.Mutex{}
		s.maxSavedProfilesPerOp = maxSavedStatProfiles
		s.allProfiles = AllocOpProfiles()
		s.saveLock = sync.Mutex{}
		s.saveChannel = make(chan *utils.Profiler, saveChannelSize)
		s.internalSaveChannel = make(chan *utils.Profiler, saveChannelSize)
		s.saveProfilerProfiles = make(opProfiles, maxSavedStatProfiles)
		s.dumpRunning = false
		s.dumpLock = sync.Mutex{}
	}

	// Kick off goroutines for saving profiles
	if doProfiling {
		go opProfileSaver(&s)
		go intProfileSaver(&s)
	}

	return &s
}

func stopServerProfiling(s *Server) {
	if doProfiling {
		close(s.saveChannel)
		close(s.internalSaveChannel)
	}
}

// Read profiles off the channel and save them
func opProfileSaver(s *Server) {
	for {
		p, more := <-s.saveChannel
		if more {
			// Got a profile to save
			op := strToOpType(p.Name())

			var profiles opProfiles = s.allProfiles[op]
			success := saveProfilerInternal(&profiles, p, &s.opStatsLock, s.maxSavedProfilesPerOp)
			if !success {
				// Couldn't save, no room
				if !loggedOutOfStatsRoom[op] {
					logger.Infof("Can't save stats for op %v, no more room!", op)
					loggedOutOfStatsRoom[op] = true
				}
			}

			// If saved stats > threshold, kick off a goroutine to dump stats and free up some room
			if len(s.allProfiles[op]) >= statsCleanupThreshold {
				//logger.Infof("Calling DumpIfNecessary; num %v stats is %v, threshold is %v.", op.String(), len(s.allProfiles[op]), statsCleanupThreshold)
				DumpIfNecessary(s)
			}
		} else {
			// Got channel close
			//logger.Infof("Profiler save channel closed.")
			return
		}
	}
}

func intProfileSaver(s *Server) {
	for {
		p, more := <-s.internalSaveChannel
		if more {
			// Got a profile to save
			var saveProfiles opProfiles = s.saveProfilerProfiles
			saveProfilerInternal(&saveProfiles, p, &s.saveLock, s.maxSavedProfilesPerOp)
		} else {
			// Got channel close
			//logger.Infof("Profiler internal save channel closed.")
			return
		}
	}
}

// Saves a profile, if there is room. Returns true on success and false if out of room
func saveProfilerInternal(profiles *opProfiles, profiler *utils.Profiler, profLock *sync.Mutex, maxProfiles int) bool {
	rtnVal := true // success

	profLock.Lock()
	statsIndex := len(*profiles)
	if statsIndex < maxProfiles {
		(*profiles)[statsIndex] = profiler
	} else {
		// Don't save, no room. Indicate failed to save
		rtnVal = false
	}
	profLock.Unlock()

	return rtnVal
}

// This is purposely not a method in Server, since if is is one then
// net/rpc/server.go complains that it has the wrong number of ins/outs,
// from a check in suitableMethods().
func SaveProfiler(s *Server, op OpType, profiler *utils.Profiler) {
	if (op == InvalidOp) || (profiler == nil) {
		return
	}

	// Set op name, in case it wasn't known when the profiler was created
	profiler.SetName(op.String())

	// Profile myself...
	var profile_myself *utils.Profiler = nil
	if doInternalProfiling {
		profile_myself = utils.NewProfilerIf(doProfiling, "SaveProfiler")
	}

	// Save profiler by writing its pointer to the save channel
	s.saveChannel <- profiler

	// Save my own profiling
	if doInternalProfiling {
		profile_myself.Close()
		// Save profiler by writing its pointer to the internal save channel
		s.internalSaveChannel <- profile_myself
	}
}

// XXX TODO: create conf setting for this?
var dumpStatsProfilesToLog bool = true

func DumpProfileMap(profilerMap *opProfiles) {

	if len(*profilerMap) > 0 {
		stats := utils.GetStats(*profilerMap)
		if dumpStatsProfilesToLog {
			statsStr := stats.DumpToString(true)
			logger.Infof("Periodic stats: %s", statsStr)
		} else {
			stats.DumpToStdout()
		}
	}
}

var doInternalProfiling bool = false
var doProfiling bool = false
var dumpInternalStats bool = false

func dumpStats(s *Server) {

	// Dump stats; looping over stats for each op
	for _, opProfs := range s.allProfiles {
		DumpProfileMap(&opProfs)
	}

	if dumpInternalStats {
		DumpProfileMap(&s.saveProfilerProfiles)
	}
}

func DumpIfNecessary(s *Server) {
	if !doProfiling {
		return
	}

	// First check to see if running already
	s.dumpLock.Lock()
	alreadyRunning := s.dumpRunning
	s.dumpLock.Unlock()

	if !alreadyRunning {
		//logger.Infof("Triggering stats dump and free.")
		go DumpAndFreeStats(s)
	}
}

// How to clean up? Move profiler to some stale list? Just delete?
func DumpAndFreeStats(s *Server) {
	// Prevent multiple instances from running at the same time (?)
	// If so, should we check to see if there's anything to do first?
	s.dumpLock.Lock()
	alreadyRunning := s.dumpRunning
	if !alreadyRunning {
		s.dumpRunning = true
	}
	s.dumpLock.Unlock()

	if alreadyRunning {
		//logger.Infof("Dump is already running; return.")
	}

	var profiler = utils.NewProfilerIf(doProfiling, "DumpAndFreeStats")

	// Lock
	s.opStatsLock.Lock()
	profiler.AddEventNow("after lock")

	// XXX TODO: Is it faster to copy/free and then dump later,
	//           or just to dump/free and return?

	// XXX TODO: do the makes outside the lock, just make it max size
	//           could make things faster in here...

	// Copy profiles to a local map
	savedOpProfiles := make(map[OpType]opProfiles, len(s.allProfiles))

	// Do a an element-by-element copy, since otherwise we get a reference to the
	// original and that is not what we want
	for op, opProfMap := range s.allProfiles {
		// Alloc space, same size as source
		savedOpProfiles[op] = make(opProfiles, len(opProfMap))

		// Deep copy
		for index, entry := range opProfMap {
			savedOpProfiles[op][index] = entry
		}
	}
	profiler.AddEventNow("after copy")

	// Free saved profiles; doing this by creating a new map, therefore removing references
	// to anything that was in the previous one.

	// NOTE: this will clear out the save and dump profiles as well!
	s.allProfiles = AllocOpProfiles()
	profiler.AddEventNow("after free")

	// Unlock
	s.opStatsLock.Unlock()
	profiler.AddEventNow("after unlock")

	// Mark ourselves as done since we're not accessing the common data structure any more.
	// XXX TODO: Is this a race condition waiting to happen?
	s.dumpLock.Lock()
	s.dumpRunning = false
	s.dumpLock.Unlock()

	// Dump stats; looping over stats for each op
	for _, opProfMap := range savedOpProfiles {
		//logger.Infof("op %v: num stats=%v\n", op, len(opProfMap))
		DumpProfileMap(&opProfMap)
	}
	profiler.AddEventNow("after dump")

	if dumpInternalStats {
		DumpProfileMap(&s.saveProfilerProfiles)
	}

	// Save my own profiling
	profiler.Close()
	// XXX TODO: Disabled dumping of this data; it's for debug only
	//profiler.Dump() // XXX TODO: There isn't a dump to string for this one yet
}

// RESPONSE ERROR FORMATTING
//
// NOTE: The format of the response error is defined in section 5.1 of the
//       JSON-RPC 2.0 Specification (http://www.jsonrpc.org/specification),
//       which looks something like this:
//
//"  5.1 Error object
//   When a rpc call encounters an error, the Response Object MUST contain the error
//   member with a value that is a Object with the following members:
//
//   code
//     A Number that indicates the error type that occurred.
//     This MUST be an integer.
//   message
//     A String providing a short description of the error.
//     The message SHOULD be limited to a concise single sentence.
//   data
//     A Primitive or Structured value that contains additional information about the error.
//     This may be omitted.
//     The value of this member is defined by the Server (e.g. detailed error information,
//     nested errors etc.).
//
//   The error codes from and including -32768 to -32000 are reserved for pre-defined errors.
//   Any code within this range, but not defined explicitly below is reserved for future use.
//"
// Editorial comment:
//   It looks that the "code" here is intended for errors specific to the RPC infrastructure
//   itself. RPC/JSON-related error values in the range of -32768 to -32000 are defined in
//   the specification. It also says that -32000 to -32099 is reserved for
//   "implementation-defined server-errors". I assume this means JSON RPC server errors.
//   Then it says "The remainder of the space is available for application defined errors".
//
// I did an experiment with sending an RPC for a method that does not exist, to see if our
// JSON RPC go server returns any of these error codes (-32601 is Method not found). It did
// not send back an error code, just an error with the following text:
//     "rpc: can't find method Server.RpcGetStatStatStat"
//
// An example from random.org, a response for a request for a method that doesn't exist:
//   {
//      "jsonrpc": "2.0",
//      "error": {
//          "code": -32601,
//          "message": "Method not found",
//          "data": null
//      },
//      "id": 18197
//   }

// ERROR HANDLING/PARAMETER CHECKING
//
// Because Go does runtime type checking, it's possible that we can trigger a panic by
// passing bad/incorrect parameters into fs functions. It's important that we add the
// proper checks here or in fs code so that a bad parameter in an RPC call cannot ever
// crash proxyfsd.

// rpcEncodeError converts an error into a format that can be parsed by the client
// to extract error details.
//
// It extracts errno out of the error (if present) and encodes it into the error
// returned by the function. This will be put into the JSON RPC error field by
// the JSON RPC server, to be sent to the far end.
//
// Note that we are discarding ProxyFS-specific error text and other details here;
// this information is not useful to a remote client. If desired, one could log
// the details before overwriting them. The code to do so would look something
// like this:
//
//    // Log the file/line where the error was set
//    logger.Infof("discarding error %v from %s\n", err, blunder.SourceLine(err))
//
//    // The stacktrace can be printed, if it has one (null string is returned if not)
//    logger.Infof("discarding error %v with stacktrace %s\n", err, blunder.Stacktrace(err))
//
//    // Alternately, one could print out all the error details at once
//    logger.Infof("discarding error %v\n", err, blunder.Details(err))
//
// Error format sent to clients:
//
// Currently that format is key/value pairs separated by newline, in conf-like format:
//   <key>: <value>\n
//
// More specifically, we are passing:
//   errno: <errno int>
//
// XXX TODO: The format of how this information will be conveyed over JSON RPC has not
//           been determined; this is just an experimental implementation.
//
// XXX TODO: We should probably encode in JSON instead, since that is the way everything else
//           we send is being encoded.
//
// NOTE: e needs to be pointer to error so that we can modify it
//
func rpcEncodeError(e *error) {
	if *e != nil {
		*e = fmt.Errorf("errno: %d", blunder.Errno(*e))
	}
}

// XXX TODO: To avoid repetitive error checking with the same blunder calls, can
//           we do something like this? (from https://blog.golang.org/errors-are-values):
//
//  type errWriter struct {
//      w   io.Writer
//      err error
//  }
//
//  func (ew *errWriter) write(buf []byte) {
//      if ew.err != nil {
//          return
//      }
//      _, ew.err = ew.w.Write(buf)
//  }
//  As soon as an error occurs, the write method becomes a no-op but the error value is saved.
//
//  Given the errWriter type and its write method, the code above can be refactored:
//
//  ew := errWriter{w: fd}
//  ew.write(p0[a:b])
//  ew.write(p1[c:d])
//  ew.write(p2[e:f])
//  // and so on
//  if ew.err != nil {
//      return ew.err
//  }
//
// Editorial comment:
//     Since we aren't wrapping the same function call all the time, the exact same
//     approach can't be used. But can we create something that can track the error
//     and check it before each call?
//
//     Could use function variable or something, but would that make the code
//     too hard to read?
// OR...
//     go actually has a goto statement; that might be easier to follow than an obscure
//     mechanism, while still allowing us to only write the blunder call in one place
//     per function.
//     BUT you can't jump over variable declarations, just like in C.
// OR...
//     create a deferred function that runs on exit and does the error handling?
//     have to be careful with the syntax so that the final version of err is
//     passed to the onExit function.

/*
type errTracker struct {
	err error
}

func (et *errTracker) call() {
	if et.err != nil {
		return
	}

	// Otherwise call the function we were passed?
}

// XXX TODO: Alternate error-handling approach - use goto so that error handling
//           code only appears once in a given function.
//           Downside: can't jump over variable creation (i.e. can't use :=)
func (s *Server) RpcLinkPath(in *LinkPathRequest, reply *Reply) (err error) {
	flog := logger.TraceEnter("in.", in)
    defer func() {  flog.TraceExitErr("reply.", err, reply) }()

    var src_ino, tgt_ino inode.InodeNumber

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	// Split fullpath into parent dir and basename
	parentDir, basename := splitPath(in.Fullpath)

	// Get the inode for the (source) parent dir
	src_ino, err = mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeGroupID(0), nil, parentDir)
	if err != nil {
		goto Done
	}

	// Get the inode for the target dir
	tgt_ino, err = mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeGroupID(0), nil, in.TargetFullpath)
	if err != nil {
		goto Done
	}

	// Do the link
	err = mountHandle.Link(inode.InodeRootUserID, inode.InodeGroupID(0), nil, src_ino, basename, tgt_ino)

Done:
	err = fmt.Errorf("errno: %d", blunder.Errno(err))
	return
}
*/

// Shorthand for our internal API debug log id; global to the package
const internalDebug = logger.DbgInternal

func splitPath(fullpath string) (parentDir string, basename string) {
	// Split fullpath into parent dir and new basename
	parentDir = filepath.Dir(fullpath)
	basename = filepath.Base(fullpath)
	return
}

func (s *Server) RpcChown(in *ChownRequest, reply *Reply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	// NOTE: We currently just store and return per-inode ownership info.
	//       We do not check/enforce it; that is the caller's responsibility.

	stat := make(fs.Stat)
	if in.UserID != -1 {
		stat[fs.StatUserID] = uint64(in.UserID)
	}
	if in.GroupID != -1 {
		stat[fs.StatGroupID] = uint64(in.GroupID)
	}
	err = mountHandle.Setstat(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(in.InodeNumber), stat)
	return
}

func (s *Server) RpcChownPath(in *ChownPathRequest, reply *Reply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	// NOTE: We currently just store and return per-inode ownership info.
	//       We do not check/enforce it; that is the caller's responsibility.

	// Get the inode
	ino, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeGroupID(0), nil, in.Fullpath)
	if err != nil {
		return
	}

	// Do the Setstat
	stat := make(fs.Stat)
	if in.UserID != -1 {
		stat[fs.StatUserID] = uint64(in.UserID)
	}
	if in.GroupID != -1 {
		stat[fs.StatGroupID] = uint64(in.GroupID)
	}
	err = mountHandle.Setstat(inode.InodeRootUserID, inode.InodeGroupID(0), nil, ino, stat)
	return
}

func (s *Server) RpcChmod(in *ChmodRequest, reply *Reply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	// NOTE: We currently just store and return per-inode ownership info.
	//       We do not check/enforce it; that is the caller's responsibility.

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	// Samba includes the file mode in in.FileMode, but only the permssion
	// bits can be changed by SetStat().
	stat := make(fs.Stat)
	stat[fs.StatMode] = uint64(in.FileMode) & 07777
	err = mountHandle.Setstat(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(in.InodeNumber), stat)
	return
}

func (s *Server) RpcChmodPath(in *ChmodPathRequest, reply *Reply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	// NOTE: We currently just store and return per-inode ownership info.
	//       We do not check/enforce it; that is the caller's responsibility.

	// Get the inode
	ino, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeGroupID(0), nil, in.Fullpath)
	if err != nil {
		return
	}

	// Do the Setstat
	//
	// Samba includes the file mode in in.FileMode, but only the permssion
	// bits can be changed by SetStat().
	stat := make(fs.Stat)
	stat[fs.StatMode] = uint64(in.FileMode) & 07777
	err = mountHandle.Setstat(inode.InodeRootUserID, inode.InodeGroupID(0), nil, ino, stat)
	return
}

func (s *Server) RpcCreate(in *CreateRequest, reply *InodeReply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	fino, err := mountHandle.Create(inode.InodeUserID(in.UserID), inode.InodeGroupID(in.GroupID), nil, inode.InodeNumber(in.InodeNumber), in.Basename, inode.InodeMode(in.FileMode))
	reply.InodeNumber = int64(uint64(fino))
	return
}

func (s *Server) RpcCreatePath(in *CreatePathRequest, reply *InodeReply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	// Ideally we would like all name/fullpath checking logic to be in the fs package,
	// however since the fs.Create() and fs.Mkdir() APIs are inode-based, once we are
	// inside those functions the fullpath is no longer available. The simplest solution is to
	// just do the checking here.
	err = fs.ValidateFullPath(in.Fullpath)
	if err != nil {
		return err
	}

	// Split fullpath into parent dir and new basename
	parentDir, basename := splitPath(in.Fullpath)

	// Get the inode for the parent dir
	ino, err := mountHandle.LookupPath(inode.InodeUserID(in.UserID), inode.InodeGroupID(in.GroupID), nil, parentDir)
	if err != nil {
		return
	}

	// Do the create
	fino, err := mountHandle.Create(inode.InodeUserID(in.UserID), inode.InodeGroupID(in.GroupID), nil, ino, basename, inode.InodeMode(in.FileMode))
	reply.InodeNumber = int64(uint64(fino))
	return
}

func (s *Server) RpcFlock(in *FlockRequest, reply *FlockReply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	var flock fs.FlockStruct
	flock.Type = in.FlockType
	flock.Whence = in.FlockWhence
	flock.Start = in.FlockStart
	flock.Len = in.FlockLen
	flock.Pid = in.FlockPid

	lockStruct, err := mountHandle.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(in.InodeNumber), in.FlockCmd, &flock)
	if lockStruct != nil {
		reply.FlockType = lockStruct.Type
		reply.FlockWhence = lockStruct.Whence
		reply.FlockStart = lockStruct.Start
		reply.FlockLen = lockStruct.Len
		reply.FlockPid = lockStruct.Pid
	}
	return
}

func UnixSec(t time.Time) (sec int64) {
	return t.Unix()
}

func UnixNanosec(t time.Time) (ns int64) {
	return t.UnixNano() - t.Unix()*int64(time.Second)
}

func (s *Server) RpcFlush(in *FlushRequest, reply *Reply) (err error) {
	var profiler = utils.NewProfilerIf(doProfiling, "flush")

	globals.gate.RLock()
	defer globals.gate.RUnlock()

	sendTime := time.Unix(in.SendTimeSec, in.SendTimeNsec)
	requestRecTime := time.Now()
	deliveryLatency := requestRecTime.Sub(sendTime)
	deliveryLatencyUsec := deliveryLatency.Nanoseconds()
	var flog logger.FuncCtx

	if globals.dataPathLogging {
		flog = logger.TraceEnter("in.", in, "deliveryLatencyUsec:"+strconv.FormatInt(deliveryLatencyUsec, 10))
	}

	stopwatch := utils.NewStopwatch()
	defer func() {
		_ = stopwatch.Stop()
		if globals.dataPathLogging {
			flog.TraceExitErr("reply.", err, reply, "duration:"+stopwatch.ElapsedMsString())
		}
	}()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	profiler.AddEventNow("before fs.Flush()")
	mountHandle, err := lookupMountHandle(in.MountID)
	if nil == err {
		err = mountHandle.Flush(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(in.InodeNumber))
	}
	profiler.AddEventNow("after fs.Flush()")

	reply.RequestTimeSec = UnixSec(requestRecTime)
	reply.RequestTimeNsec = UnixNanosec(requestRecTime)

	replySendTime := time.Now()
	reply.SendTimeSec = replySendTime.Unix()
	reply.SendTimeNsec = (replySendTime.UnixNano() - (reply.SendTimeSec * int64(time.Second)))

	// Save profiler with server op stats
	profiler.Close()
	SaveProfiler(s, FlushOp, profiler)

	return
}

func (stat *StatStruct) fsStatToStatStruct(fsStat fs.Stat) {
	stat.CRTimeNs = fsStat[fs.StatCRTime]
	stat.CTimeNs = fsStat[fs.StatCTime]
	stat.MTimeNs = fsStat[fs.StatMTime]
	stat.ATimeNs = fsStat[fs.StatATime]
	stat.Size = fsStat[fs.StatSize]
	stat.NumLinks = fsStat[fs.StatNLink]
	stat.StatInodeNumber = int64(fsStat[fs.StatINum])
	stat.FileMode = uint32(fsStat[fs.StatMode])
	stat.UserID = uint32(fsStat[fs.StatUserID])
	stat.GroupID = uint32(fsStat[fs.StatGroupID])
}

func (s *Server) RpcGetStat(in *GetStatRequest, reply *StatStruct) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	var stat fs.Stat
	var profiler = utils.NewProfilerIf(doProfiling, "getstat")

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	profiler.AddEventNow("before fs.Getstat()")
	mountHandle, err := lookupMountHandle(in.MountID)
	if nil == err {
		stat, err = mountHandle.Getstat(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(in.InodeNumber))
	}
	profiler.AddEventNow("after fs.Getstat()")
	if err == nil {
		reply.fsStatToStatStruct(stat)
	}

	// Save profiler with server op stats
	profiler.Close()
	SaveProfiler(s, GetStatOp, profiler)

	return
}

func (s *Server) RpcGetStatPath(in *GetStatPathRequest, reply *StatStruct) (err error) {
	var profiler = utils.NewProfilerIf(doProfiling, "getstat_path")

	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	// Get the inode
	profiler.AddEventNow("before fs.LookupPath()")
	ino, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeGroupID(0), nil, in.Fullpath)
	profiler.AddEventNow("after fs.LookupPath()")
	if err != nil {
		// Save profiler with server op stats
		profiler.Close()
		SaveProfiler(s, GetStatPathOp, profiler)
		return
	}

	// Do the GetStat
	profiler.AddEventNow("before fs.Getstat()")
	stat, err := mountHandle.Getstat(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(ino))
	profiler.AddEventNow("after fs.Getstat()")
	if err == nil {
		reply.fsStatToStatStruct(stat)
	}

	// Save profiler with server op stats
	profiler.Close()
	SaveProfiler(s, GetStatPathOp, profiler)

	return
}

func (s *Server) RpcGetXAttr(in *GetXAttrRequest, reply *GetXAttrReply) (err error) {
	var profiler = utils.NewProfilerIf(doProfiling, "getxattr")

	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	profiler.AddEventNow("before fs.GetXAttr()")
	mountHandle, err := lookupMountHandle(in.MountID)
	if nil == err {
		reply.AttrValue, err = mountHandle.GetXAttr(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(in.InodeNumber), in.AttrName)
	}
	profiler.AddEventNow("after fs.GetXAttr()")

	// Save profiler with server op stats
	profiler.Close()
	SaveProfiler(s, GetXattrOp, profiler)

	return
}

func (s *Server) RpcGetXAttrPath(in *GetXAttrPathRequest, reply *GetXAttrReply) (err error) {
	var profiler = utils.NewProfilerIf(doProfiling, "getxattr_path")

	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	profiler.AddEventNow("before fs.LookupPath()")
	ino, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeGroupID(0), nil, in.Fullpath)
	profiler.AddEventNow("after fs.LookupPath()")
	if err != nil {
		// Save profiler with server op stats
		profiler.Close()
		SaveProfiler(s, GetXattrPathOp, profiler)
		return
	}

	profiler.AddEventNow("before fs.GetXAttr()")
	reply.AttrValue, err = mountHandle.GetXAttr(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(ino), in.AttrName)
	profiler.AddEventNow("after fs.GetXAttr()")
	if err == nil {
		reply.AttrValueSize = uint64(len(reply.AttrValue))
	}

	// Save profiler with server op stats
	profiler.Close()
	SaveProfiler(s, GetXattrPathOp, profiler)

	return
}

func (s *Server) RpcLog(in *LogRequest, reply *Reply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	logger.Info(in.Message)
	return
}

func (s *Server) RpcLookupPath(in *LookupPathRequest, reply *InodeReply) (err error) {
	var profiler = utils.NewProfilerIf(doProfiling, "lookup_path")

	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	profiler.AddEventNow("before fs.LookupPath()")
	ino, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeGroupID(0), nil, in.Fullpath)
	profiler.AddEventNow("after fs.LookupPath()")
	if err == nil {
		reply.InodeNumber = int64(uint64(ino))
	}

	// Save profiler with server op stats
	profiler.Close()
	SaveProfiler(s, LookupPathOp, profiler)

	return
}

func (s *Server) RpcLink(in *LinkRequest, reply *Reply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	err = mountHandle.Link(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(in.InodeNumber), in.Basename, inode.InodeNumber(in.TargetInodeNumber))
	return
}

func (s *Server) RpcLinkPath(in *LinkPathRequest, reply *Reply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	// Split fullpath into parent dir and basename
	parentDir, basename := splitPath(in.Fullpath)

	// Get the inode for the (source) parent dir
	srcIno, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeGroupID(0), nil, parentDir)
	if err != nil {
		return
	}

	// Get the inode for the target
	tgtIno, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeGroupID(0), nil, in.TargetFullpath)
	if err != nil {
		return
	}

	// Do the link
	err = mountHandle.Link(inode.InodeRootUserID, inode.InodeGroupID(0), nil, srcIno, basename, tgtIno)
	return
}

func (s *Server) RpcListXAttr(in *ListXAttrRequest, reply *ListXAttrReply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	reply.AttrNames, err = mountHandle.ListXAttr(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(in.InodeNumber))
	return
}

func (s *Server) RpcListXAttrPath(in *ListXAttrPathRequest, reply *ListXAttrReply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	ino, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeGroupID(0), nil, in.Fullpath)
	if err != nil {
		return
	}

	reply.AttrNames, err = mountHandle.ListXAttr(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(ino))
	if err != nil {
		return
	}

	return
}

func (s *Server) RpcLookup(in *LookupRequest, reply *InodeReply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	var profiler = utils.NewProfilerIf(doProfiling, "lookup")

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	profiler.AddEventNow("before fs.Lookup()")
	ino, err := mountHandle.Lookup(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(in.InodeNumber), in.Basename)
	profiler.AddEventNow("after fs.Lookup()")
	// line below is for testing fault injection
	//err = blunder.AddError(err, blunder.TryAgainError)
	if err == nil {
		reply.InodeNumber = int64(uint64(ino))
	}

	// Save profiler with server op stats
	profiler.Close()
	SaveProfiler(s, LookupOp, profiler)

	return
}

func (s *Server) RpcMkdir(in *MkdirRequest, reply *InodeReply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	ino, err := mountHandle.Mkdir(inode.InodeUserID(in.UserID), inode.InodeGroupID(in.GroupID), nil, inode.InodeNumber(in.InodeNumber), in.Basename, inode.InodeMode(in.FileMode))
	reply.InodeNumber = int64(uint64(ino))
	return
}

func (s *Server) RpcMkdirPath(in *MkdirPathRequest, reply *Reply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	// Ideally we would like all name/fullpath checking logic to be in the fs package,
	// however since the fs.Create() and fs.Mkdir() APIs are inode-based, once we are
	// inside those functions the fullpath is no longer available. The simplest solution is to
	// just do the checking here.
	err = fs.ValidateFullPath(in.Fullpath)
	if err != nil {
		return err
	}

	// Split fullpath into parent dir and new basename
	parentDir, basename := splitPath(in.Fullpath)

	// Get the inode for the parent dir
	ino, err := mountHandle.LookupPath(inode.InodeUserID(in.UserID), inode.InodeGroupID(in.GroupID), nil, parentDir)
	if err != nil {
		return
	}

	// Do the mkdir
	_, err = mountHandle.Mkdir(inode.InodeUserID(in.UserID), inode.InodeGroupID(in.GroupID), nil, ino, basename, inode.InodeMode(in.FileMode))
	return
}

func (s *Server) RpcMount(in *MountRequest, reply *MountReply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := fs.Mount(in.VolumeName, fs.MountOptions(in.MountOptions))
	if err == nil {
		reply.MountID = allocateMountID(mountHandle)
		reply.RootDirInodeNumber = int64(uint64(inode.RootDirInodeNumber))
	}
	return
}

func (s *Server) RpcRead(in *ReadRequest, reply *ReadReply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	sendTime := time.Unix(in.SendTimeSec, in.SendTimeNsec)
	requestRecTime := time.Now()
	deliveryLatency := requestRecTime.Sub(sendTime)
	deliveryLatencyUsec := deliveryLatency.Nanoseconds() / int64(time.Microsecond)
	var flog logger.FuncCtx

	if globals.dataPathLogging {
		// log function enter and exit, without printing read buffer
		flog = logger.TraceEnter("in.", in, "deliveryLatencyUsec:"+strconv.FormatInt(deliveryLatencyUsec, 10))
	}

	stopwatch := utils.NewStopwatch()
	defer func() {
		_ = stopwatch.Stop()
		if globals.dataPathLogging {
			flog.TraceExitErr("reply.", err,
				"Buf.size:"+strconv.Itoa(len(reply.Buf))+", Buf.<buffer not printed>",
				"reply.SendTimeSec:"+strconv.FormatInt(reply.SendTimeSec, 10),
				"reply.SendTimeNsec:"+strconv.FormatInt(reply.SendTimeNsec, 10),
				"reply.RequestTimeSec:"+strconv.FormatInt(reply.RequestTimeSec, 10),
				"reply.RequestTimeNsec:"+strconv.FormatInt(reply.RequestTimeNsec, 10),
				"duration:"+stopwatch.ElapsedMsString(),
			)
		}
	}()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil == err {
		reply.Buf, err = mountHandle.Read(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(in.InodeNumber), in.Offset, in.Length, nil)
	}

	reply.RequestTimeSec = UnixSec(requestRecTime)
	reply.RequestTimeNsec = UnixNanosec(requestRecTime)

	replySendTime := time.Now()
	reply.SendTimeSec = UnixSec(replySendTime)
	reply.SendTimeNsec = UnixNanosec(replySendTime)
	return
}

func (dirEnt *DirEntry) fsDirentToDirEntryStruct(fsDirent inode.DirEntry) {
	dirEnt.InodeNumber = int64(uint64(fsDirent.InodeNumber))
	dirEnt.Basename = fsDirent.Basename
	dirEnt.FileType = uint16(fsDirent.Type)
	dirEnt.NextDirLocation = int64(fsDirent.NextDirLocation)
}

func (s *Server) RpcReaddir(in *ReaddirRequest, reply *ReaddirReply) (err error) {
	profiler := utils.NewProfilerIf(doProfiling, "readdir")
	err = s.rpcReaddirInternal(in, reply, profiler)
	// Save profiler with server op stats
	profiler.Close()
	SaveProfiler(s, ReaddirOp, profiler)
	return
}

func (s *Server) RpcReaddirByLoc(in *ReaddirByLocRequest, reply *ReaddirReply) (err error) {
	profiler := utils.NewProfilerIf(doProfiling, "readdirByLoc")
	err = s.rpcReaddirInternal(in, reply, profiler)
	// Save profiler with server op stats
	profiler.Close()
	SaveProfiler(s, ReaddirByLocOp, profiler)
	return
}

func (s *Server) rpcReaddirInternal(in interface{}, reply *ReaddirReply, profiler *utils.Profiler) (err error) {
	var iH InodeHandle
	var prevMarker interface{}
	var inByLoc *ReaddirByLocRequest
	var flog logger.FuncCtx

	inByName, okByName := in.(*ReaddirRequest)
	if okByName {
		iH = inByName.InodeHandle
		prevMarker = inByName.PrevDirEntName
		flog = logger.TraceEnter("in.", inByName)
	} else {
		inByLoc, _ = in.(*ReaddirByLocRequest)
		iH = inByLoc.InodeHandle
		prevMarker = inode.InodeDirLocation(inByLoc.PrevDirEntLocation)
		flog = logger.TraceEnter("in.", inByLoc)
	}

	globals.gate.RLock()
	defer globals.gate.RUnlock()

	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(iH.MountID)
	if nil != err {
		return
	}

	profiler.AddEventNow("before fs.ReaddirOne()")
	dirEnts, err := mountHandle.ReaddirOne(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(iH.InodeNumber), prevMarker)
	profiler.AddEventNow("after fs.ReaddirOne()")
	if err == nil {
		reply.DirEnts = make([]DirEntry, len(dirEnts))
		for i := range dirEnts {
			reply.DirEnts[i].fsDirentToDirEntryStruct(dirEnts[i])
		}
	}

	return
}

func (s *Server) RpcReaddirPlus(in *ReaddirPlusRequest, reply *ReaddirPlusReply) (err error) {
	profiler := utils.NewProfilerIf(doProfiling, "readdir_plus")
	err = s.rpcReaddirPlusInternal(in, reply, profiler)
	// Save profiler with server op stats
	profiler.Close()
	SaveProfiler(s, ReaddirPlusOp, profiler)
	return
}

func (s *Server) RpcReaddirPlusByLoc(in *ReaddirPlusByLocRequest, reply *ReaddirPlusReply) (err error) {
	profiler := utils.NewProfilerIf(doProfiling, "readdir_plus_by_loc")
	err = s.rpcReaddirPlusInternal(in, reply, profiler)
	// Save profiler with server op stats
	profiler.Close()
	SaveProfiler(s, ReaddirPlusByLocOp, profiler)
	return
}

func (s *Server) rpcReaddirPlusInternal(in interface{}, reply *ReaddirPlusReply, profiler *utils.Profiler) (err error) {
	var iH InodeHandle
	var prevMarker interface{}
	var flog logger.FuncCtx

	inByName, okByName := in.(*ReaddirPlusRequest)
	if okByName {
		iH = inByName.InodeHandle
		prevMarker = inByName.PrevDirEntName
		flog = logger.TraceEnter("in.", inByName)
	} else {
		inByLoc, _ := in.(*ReaddirPlusByLocRequest)
		iH = inByLoc.InodeHandle
		prevMarker = inode.InodeDirLocation(inByLoc.PrevDirEntLocation)
		flog = logger.TraceEnter("in.", inByLoc)
	}

	globals.gate.RLock()
	defer globals.gate.RUnlock()

	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(iH.MountID)
	if err != nil {
		return
	}

	profiler.AddEventNow("before fs.ReaddirOnePlus()")
	dirEnts, statEnts, err := mountHandle.ReaddirOnePlus(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(iH.InodeNumber), prevMarker)

	profiler.AddEventNow("after fs.ReaddirOnePlus()")
	if err == nil {
		reply.DirEnts = make([]DirEntry, len(dirEnts))
		reply.StatEnts = make([]StatStruct, len(dirEnts))
		for i := range dirEnts {
			// Fill in dirents
			reply.DirEnts[i].fsDirentToDirEntryStruct(dirEnts[i])

			// Fill in stats
			reply.StatEnts[i].fsStatToStatStruct(statEnts[i])
		}
	}

	return
}

func (s *Server) RpcReadSymlink(in *ReadSymlinkRequest, reply *ReadSymlinkReply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	target, err := mountHandle.Readsymlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(in.InodeNumber))
	reply.Target = target
	return
}

func (s *Server) RpcReadSymlinkPath(in *ReadSymlinkPathRequest, reply *ReadSymlinkReply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	// Get the inode
	ino, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeGroupID(0), nil, in.Fullpath)
	if err != nil {
		return
	}

	target, err := mountHandle.Readsymlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil, ino)
	reply.Target = target
	return
}

func (s *Server) RpcRemovetXAttr(in *RemoveXAttrRequest, reply *Reply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	err = mountHandle.RemoveXAttr(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(in.InodeNumber), in.AttrName)
	return
}

func (s *Server) RpcRemoveAttrPath(in *RemoveXAttrPathRequest, reply *Reply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	ino, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeGroupID(0), nil, in.Fullpath)
	if err != nil {
		return
	}

	err = mountHandle.RemoveXAttr(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(ino), in.AttrName)
	return
}

func (s *Server) RpcRename(in *RenameRequest, reply *Reply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	err = mountHandle.Rename(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(in.SrcDirInodeNumber), in.SrcBasename, inode.InodeNumber(in.DstDirInodeNumber), in.DstBasename)
	return
}

func (s *Server) RpcRenamePath(in *RenamePathRequest, reply *Reply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	// Split fullpath into (source) parent dir and new basename
	srcParentDir, srcBasename := splitPath(in.Fullpath)

	// Get the inode for the (source) parent dir
	srcIno, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeGroupID(0), nil, srcParentDir)
	if err != nil {
		return
	}

	// Split DstFullpath into (source) parent dir and new basename
	dstParentDir, dstBasename := splitPath(in.DstFullpath)

	// Get the inode for the dest parent dir
	dstIno, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeGroupID(0), nil, dstParentDir)
	if err != nil {
		return
	}

	// Do the rename
	err = mountHandle.Rename(inode.InodeRootUserID, inode.InodeGroupID(0), nil, srcIno, srcBasename, dstIno, dstBasename)
	return
}

func (s *Server) RpcResize(in *ResizeRequest, reply *Reply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	err = mountHandle.Resize(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(in.InodeNumber), in.NewSize)
	return
}

func (s *Server) RpcRmdir(in *UnlinkRequest, reply *Reply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	err = mountHandle.Rmdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(in.InodeNumber), in.Basename)
	return
}

func (s *Server) RpcRmdirPath(in *UnlinkPathRequest, reply *Reply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	// Split fullpath into parent dir and new basename
	parentDir, basename := splitPath(in.Fullpath)

	// Get the inode for the parent dir
	ino, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeGroupID(0), nil, parentDir)
	if err != nil {
		return
	}

	// Do the rmdir
	err = mountHandle.Rmdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil, ino, basename)
	return
}

func (s *Server) RpcSetstat(in *SetstatRequest, reply *Reply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	stat := make(fs.Stat)
	stat[fs.StatCRTime] = in.CRTimeNs
	stat[fs.StatCTime] = in.CTimeNs
	stat[fs.StatMTime] = in.MTimeNs
	stat[fs.StatATime] = in.ATimeNs
	stat[fs.StatSize] = in.Size
	stat[fs.StatNLink] = in.NumLinks
	// XXX TODO: add in mode/userid/groupid?
	err = mountHandle.Setstat(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(in.InodeNumber), stat)
	return
}

func (s *Server) RpcSetTime(in *SetTimeRequest, reply *Reply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	stat := make(fs.Stat)
	stat[fs.StatMTime] = in.MTimeNs
	stat[fs.StatATime] = in.ATimeNs
	err = mountHandle.Setstat(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(in.InodeNumber), stat)
	return
}

func (s *Server) RpcSetTimePath(in *SetTimePathRequest, reply *Reply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	// Get the inode
	ino, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeGroupID(0), nil, in.Fullpath)
	if err != nil {
		return
	}

	stat := make(fs.Stat)
	stat[fs.StatMTime] = in.MTimeNs
	stat[fs.StatATime] = in.ATimeNs
	err = mountHandle.Setstat(inode.InodeRootUserID, inode.InodeGroupID(0), nil, ino, stat)
	return
}

func (s *Server) RpcSetXAttr(in *SetXAttrRequest, reply *Reply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	err = mountHandle.SetXAttr(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(in.InodeNumber), in.AttrName, in.AttrValue, in.AttrFlags)
	return
}

func (s *Server) RpcSetXAttrPath(in *SetXAttrPathRequest, reply *Reply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	ino, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeGroupID(0), nil, in.Fullpath)
	if err != nil {
		return
	}

	err = mountHandle.SetXAttr(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(ino), in.AttrName, in.AttrValue, in.AttrFlags)
	return
}

func (s *Server) RpcStatVFS(in *StatVFSRequest, reply *StatVFS) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	statvfs, err := mountHandle.StatVfs()
	if err != nil {
		return
	}

	// Fill out the stats in the reply
	reply.BlockSize = statvfs[fs.StatVFSBlockSize]
	reply.FragmentSize = statvfs[fs.StatVFSFragmentSize]
	reply.TotalBlocks = statvfs[fs.StatVFSTotalBlocks]
	reply.FreeBlocks = statvfs[fs.StatVFSFreeBlocks]
	reply.AvailBlocks = statvfs[fs.StatVFSAvailBlocks]
	reply.TotalInodes = statvfs[fs.StatVFSTotalInodes]
	reply.FreeInodes = statvfs[fs.StatVFSFreeInodes]
	reply.AvailInodes = statvfs[fs.StatVFSAvailInodes]
	reply.FileSystemID = statvfs[fs.StatVFSFilesystemID]
	reply.MountFlags = statvfs[fs.StatVFSMountFlags]
	reply.MaxFilenameLen = statvfs[fs.StatVFSMaxFilenameLen]

	return
}

func (s *Server) RpcSymlink(in *SymlinkRequest, reply *Reply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	_, err = mountHandle.Symlink(inode.InodeUserID(in.UserID), inode.InodeGroupID(in.GroupID), nil, inode.InodeNumber(in.InodeNumber), in.Basename, in.Target)
	return
}

func (s *Server) RpcSymlinkPath(in *SymlinkPathRequest, reply *Reply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	// Split fullpath into (source) parent dir and new basename
	srcParentDir, srcBasename := splitPath(in.Fullpath)

	// Get the inode for the (source) parent dir
	srcIno, err := mountHandle.LookupPath(inode.InodeUserID(in.UserID), inode.InodeGroupID(in.GroupID), nil, srcParentDir)
	if err != nil {
		return
	}

	_, err = mountHandle.Symlink(inode.InodeUserID(in.UserID), inode.InodeGroupID(in.GroupID), nil, srcIno, srcBasename, in.TargetFullpath)
	return
}

func (s *Server) RpcType(in *TypeRequest, reply *TypeReply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	ftype, err := mountHandle.GetType(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(in.InodeNumber))
	// Cast as a uint16 here to get the underlying DT_* constant
	reply.FileType = uint16(ftype)
	return
}

func (s *Server) RpcUnlink(in *UnlinkRequest, reply *Reply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	err = mountHandle.Unlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(in.InodeNumber), in.Basename)
	return
}

func (s *Server) RpcUnlinkPath(in *UnlinkPathRequest, reply *Reply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil != err {
		return
	}

	// Split fullpath into parent dir and new basename
	parentDir, basename := splitPath(in.Fullpath)

	// Get the inode for the parent dir
	ino, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeGroupID(0), nil, parentDir)
	if err != nil {
		return
	}

	// Do the unlink
	err = mountHandle.Unlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil, ino, basename)
	return
}

func (s *Server) RpcWrite(in *WriteRequest, reply *WriteReply) (err error) {
	var size uint64

	globals.gate.RLock()
	defer globals.gate.RUnlock()

	sendTime := time.Unix(in.SendTimeSec, in.SendTimeNsec)
	requestRecTime := time.Now()
	deliveryLatency := requestRecTime.Sub(sendTime)
	deliveryLatencyUsec := deliveryLatency.Nanoseconds() / int64(time.Microsecond)
	var flog logger.FuncCtx

	if globals.dataPathLogging {

		// log function enter and exit, without printing write buffer
		flog = logger.TraceEnter("in.", in.InodeHandle,
			"in.Offset:"+strconv.FormatUint(in.Offset, 10),
			"in.Buf.size:"+strconv.Itoa(len(in.Buf)),
			"in.SendTimeSec:"+strconv.FormatInt(in.SendTimeSec, 10),
			"in.SendTimeNsec:"+strconv.FormatInt(in.SendTimeNsec, 10),
			"in.ReceiveTimeSec:"+strconv.FormatInt(UnixSec(requestRecTime), 10),
			"in.RecTimeNs:"+strconv.FormatInt(UnixNanosec(requestRecTime), 10),
			"deliveryLatencyUsec:"+strconv.FormatInt(deliveryLatencyUsec, 10),
		)
	}

	stopwatch := utils.NewStopwatch()
	defer func() {
		_ = stopwatch.Stop()
		if globals.dataPathLogging {
			flog.TraceExitErr("reply.", err, reply, "duration:"+stopwatch.ElapsedMsString())
		}
	}()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	mountHandle, err := lookupMountHandle(in.MountID)
	if nil == err {
		size, err = mountHandle.Write(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(in.InodeNumber), in.Offset, in.Buf, nil)
		reply.Size = uint64(size)
	}

	reply.RequestTimeSec = UnixSec(requestRecTime)
	reply.RequestTimeNsec = UnixNanosec(requestRecTime)

	replySendTime := time.Now()
	reply.SendTimeSec = UnixSec(replySendTime)
	reply.SendTimeNsec = UnixNanosec(replySendTime)
	return
}
