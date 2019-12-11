# fission

Go package enabling the implementation of a multi-threaded low-level FUSE file system.

## API Reference

```
package fission

// Volume represents a file system instance. A Volume is provisioned by calling
// NewVolume(). After recording the returned interface from NewVolume(), a single
// call to DoMount() kicks off the mounting process and the caller should expect
// to see the various callbacks listed in the Callbacks interface. This will
// continue a single call to DoUnmount() is made after which the Volume instance
// may be safely discarded.
//
type Volume interface {
	// DoMount is invoked on a Volume interface to perform the FUSE mount and
	// begin receiving the various callbacks listed in the Callbacks interface.
	//
	DoMount() (err error)

	// DoUnmount is invoked on a Volume interface to perform the FUSE unmount.
	// Upon return, no callbacks will be made and the Volume instance may be
	// safely discarded.
	//
	DoUnmount() (err error)
}

// Callbacks is the interface declaring the various callbacks that will be issued
// in response to a Volume instance while it is mounted. Note that some callbacks
// are expected to return both an error as well as a struct pointer. In the case of an
// error, the struct pointer should be <nil> as it will not be written to /dev/fuse.
// Finally, one callback is special: DoInit(). Provisioning a Volume instance involved
// providing the InitOut.MaxWrite to allow configuring the buffer pool used by the
// /dev/fuse read loop (including, of course, the reception of the InitIn up-call).
// The implementation of DoInit, therefore, should not expect returning an InitOut
// with a different MaxWrite to be honored.
//
type Callbacks interface {
	DoLookup(inHeader *InHeader, lookupIn *LookupIn) (lookupOut *LookupOut, errno syscall.Errno)
	DoForget(inHeader *InHeader, forgetIn *ForgetIn)
	DoGetAttr(inHeader *InHeader, getAttrIn *GetAttrIn) (getAttrOut *GetAttrOut, errno syscall.Errno)
	DoSetAttr(inHeader *InHeader, setAttrIn *SetAttrIn) (setAttrOut *SetAttrOut, errno syscall.Errno)
	DoReadLink(inHeader *InHeader) (readLinkOut *ReadLinkOut, errno syscall.Errno)
	DoSymLink(inHeader *InHeader, symLinkIn *SymLinkIn) (symLinkOut *SymLinkOut, errno syscall.Errno)
	DoMkNod(inHeader *InHeader, mkNodIn *MkNodIn) (mkNodOut *MkNodOut, errno syscall.Errno)
	DoMkDir(inHeader *InHeader, mkDirIn *MkDirIn) (mkDirOut *MkDirOut, errno syscall.Errno)
	DoUnlink(inHeader *InHeader, unlinkIn *UnlinkIn) (errno syscall.Errno)
	DoRmDir(inHeader *InHeader, rmDirIn *RmDirIn) (errno syscall.Errno)
	DoRename(inHeader *InHeader, renameIn *RenameIn) (errno syscall.Errno)
	DoLink(inHeader *InHeader, linkIn *LinkIn) (linkOut *LinkOut, errno syscall.Errno)
	DoOpen(inHeader *InHeader, openIn *OpenIn) (openOut *OpenOut, errno syscall.Errno)
	DoRead(inHeader *InHeader, readIn *ReadIn) (readOut *ReadOut, errno syscall.Errno)
	DoWrite(inHeader *InHeader, writeIn *WriteIn) (writeOut *WriteOut, errno syscall.Errno)
	DoStatFS(inHeader *InHeader) (statFSOut *StatFSOut, errno syscall.Errno)
	DoRelease(inHeader *InHeader, releaseIn *ReleaseIn) (errno syscall.Errno)
	DoFSync(inHeader *InHeader, fSyncIn *FSyncIn) (errno syscall.Errno)
	DoSetXAttr(inHeader *InHeader, setXAttrIn *SetXAttrIn) (errno syscall.Errno)
	DoGetXAttr(inHeader *InHeader, getXAttrIn *GetXAttrIn) (getXAttrOut *GetXAttrOut, errno syscall.Errno)
	DoListXAttr(inHeader *InHeader, listXAttrIn *ListXAttrIn) (listXAttrOut *ListXAttrOut, errno syscall.Errno)
	DoRemoveXAttr(inHeader *InHeader, removeXAttrIn *RemoveXAttrIn) (errno syscall.Errno)
	DoFlush(inHeader *InHeader, flushIn *FlushIn) (errno syscall.Errno)
	DoInit(inHeader *InHeader, initIn *InitIn) (initOut *InitOut, errno syscall.Errno)
	DoOpenDir(inHeader *InHeader, openDirIn *OpenDirIn) (openDirOut *OpenDirOut, errno syscall.Errno)
	DoReadDir(inHeader *InHeader, readDirIn *ReadDirIn) (readDirOut *ReadDirOut, errno syscall.Errno)
	DoReleaseDir(inHeader *InHeader, releaseDirIn *ReleaseDirIn) (errno syscall.Errno)
	DoFsyncDir(inHeader *InHeader, fsyncDirIn *FsyncDirIn) (errno syscall.Errno)
	DoGetLK(inHeader *InHeader, getLKIn *GetLKIn) (getLKOut *GetLKOut, errno syscall.Errno)
	DoSetLK(inHeader *InHeader, setLKIn *SetLKIn) (errno syscall.Errno)
	DoSetLKW(inHeader *InHeader, setLKWIn *SetLKWIn) (errno syscall.Errno)
	DoAccess(inHeader *InHeader, accessIn *AccessIn) (errno syscall.Errno)
	DoCreate(inHeader *InHeader, createIn *CreateIn) (createOut *CreateOut, errno syscall.Errno)
	DoInterrupt(inHeader *InHeader, interruptIn *InterruptIn)
	DoBMap(inHeader *InHeader, bMapIn *BMapIn) (bMapOut *BMapOut, errno syscall.Errno)
	DoDestroy(inHeader *InHeader)
	DoPoll(inHeader *InHeader, pollIn *PollIn) (pollOut *PollOut, errno syscall.Errno)
	DoBatchForget(inHeader *InHeader, batchForgetIn *BatchForgetIn)
	DoFAllocate(inHeader *InHeader, fAllocateIn *FAllocateIn) (errno syscall.Errno)
	DoReadDirPlus(inHeader *InHeader, readDirPlusIn *ReadDirPlusIn) (readDirPlusOut *ReadDirPlusOut, errno syscall.Errno)
	DoRename2(inHeader *InHeader, rename2In *Rename2In) (errno syscall.Errno)
	DoLSeek(inHeader *InHeader, lSeekIn *LSeekIn) (lSeekOut *LSeekOut, errno syscall.Errno)
}

// NewVolume is called to create a Volume instance. Various callbacks listed in the Callbacks interface
// will be made while the Volume is mounted. Note that the caller provides a value for InitOut.MaxWrite
// at the time the Volume instance is provisioned so that the package may properly setup the read loop
// against /dev/fuse prior to reception of an InitIn request. A chan error is also supplied to enable
// the Volume to indicate that it is no longer servicing FUSE upcalls (e.g. as a result of an intentional
// DoUnmount() call or some unexpected error reading from /dev/fuse).
//
func NewVolume(volumeName string, mountpointDirPath string, mountFlags uintptr, initOutMaxWrite uint32, callbacks Callbacks, logger *log.Logger, errChan chan error) (volume Volume)
```

## Contributors

 * ed@swiftstack.com

## License

See the included LICENSE file
