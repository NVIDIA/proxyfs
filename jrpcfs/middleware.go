package jrpcfs

import (
	"fmt"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/utils"
)

// JSON RPC Server on top of the FS package.
//
// This file handles the RPCs related to Swift middleware support
//
// NOTE: These functions should only verify the arguments and then call
// functions in package fs since there may be fs locking required.

// Utility function to mount account if not already mounted and return needed fields
func mountIfNotMounted(virtPath string) (accountName string, containerName string, objectName string, volumeName string, mountHandle fs.MountHandle, err error) {

	// Extract vAccount and vContainer from VirtPath
	accountName, containerName, objectName, err = utils.PathToAcctContObj(virtPath)
	if err != nil {
		return "", "", "", "", nil, err
	}

	// Map vAccountName to a volumeName
	volumeName, ok := fs.AccountNameToVolumeName(accountName)
	if !ok {
		err = fmt.Errorf("%v is not a recognized accountName", accountName)
		return "", "", "", "", nil, err
	}

	// Multiple middleware threads could be attempting to mount the volume at the same time.
	//
	// fs.Mount() allows this since the fs wants to support read-only and read-write mounts from Samba.
	// However, this results in two different mountHandle's for the different jrpcfs threads supporting middleware.
	//
	// Therefore, jrpcfs has to do its own serialization and store the result in globals.bimodalMountMap.
	globals.mapsLock.Lock()
	defer globals.mapsLock.Unlock()

	// Is volume mounted for this user?  If is, return the results.
	mountHandle, ok = globals.bimodalMountMap[volumeName]
	if ok {
		return accountName, containerName, objectName, volumeName, mountHandle, err
	}

	// We have not already mounted it, mount it now and store result in bimodalMountMap
	// TODO - add proper mountOpts
	mountHandle, err = fs.Mount(volumeName, fs.MountOptions(0))
	if err != nil {
		logger.DebugfIDWithError(internalDebug, err, "fs.Mount() of acct: %v container: %v failed!", accountName, containerName)
		return accountName, containerName, objectName, volumeName, nil, err
	}

	globals.bimodalMountMap[volumeName] = mountHandle
	return accountName, containerName, objectName, volumeName, mountHandle, err
}

// RpcCreateContainer is used by Middleware to PUT of a container.
//
// TODO - add update of metadata
// TODO - combine this into one PUT RPC instead of multiple RPCs?
func (s *Server) RpcCreateContainer(in *CreateContainerRequest, reply *CreateContainerReply) (err error) {
	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	// XXX TODO: Need to determine how we want to pass errors back for RPCs used by middleware.
	// By default, jrpcfs code (and rpcEncodeError) use errno-type errors.
	// However for RPCs used by middleware, perhaps we want to return HTTP status codes?
	// The blunder error package supports this, we just need to add some helper functions.
	//defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	accountName, containerName, _, _, mountHandle, err := mountIfNotMounted(in.VirtPath)

	// Validate the components of the containerName
	err = fs.ValidateFullPath(containerName)
	if err != nil {
		return err
	}

	err = fs.ValidateBaseName(containerName)
	if err != nil {
		return err
	}

	// Make the directory
	_, err = mountHandle.Mkdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.RootDirInodeNumber, containerName, inode.PosixModePerm)

	if err != nil {
		logger.DebugfIDWithError(internalDebug, err, "fs.Mkdir() of acct: %v vContainerName: %v failed!", accountName, containerName)
	}
	return
}

// RpcDelete is used by Middleware to service a DELETE HTTP request.
//
// This routine has to handle delete of an empty container as well as delete of an objectName
// where objectName could be "file1", "dir1/file1", "dir1/dir2", etc.
func (s *Server) RpcDelete(in *DeleteReq, reply *DeleteReply) (err error) {
	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	_, containerName, objectName, _, mountHandle, err := mountIfNotMounted(in.VirtPath)

	parentDir, baseName := splitPath(containerName + "/" + objectName)

	// objectName empty means we are deleting a container
	if objectName == "" {
		parentDir = "/"
		baseName = containerName
	}

	// Call fs to delete the baseName if it is a file or an empty directory.
	err = mountHandle.MiddlewareDelete(parentDir, baseName)

	return err
}

// RpcGetAccount is used by Middleware to issue a GET on an account and return the results.
func (s *Server) RpcGetAccount(in *GetAccountReq, reply *GetAccountReply) (err error) {
	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	_, _, _, _, mountHandle, err := mountIfNotMounted(in.VirtPath)
	if err != nil {
		logger.ErrorfWithError(err, "RpcGetAccount: error mounting share for %s", in.VirtPath)
		return err
	}

	entries, mtime, ctime, err := mountHandle.MiddlewareGetAccount(in.MaxEntries, in.Marker)
	if err != nil {
		return err
	}
	reply.AccountEntries = entries
	reply.ModificationTime = mtime
	reply.AttrChangeTime = ctime
	return nil
}

// RpcHead is used by Middleware to issue a GET on a container or object and return the results.
func (s *Server) RpcHead(in *HeadReq, reply *HeadReply) (err error) {
	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	_, vContainerName, vObjectName, _, mountHandle, err := mountIfNotMounted(in.VirtPath)
	if err != nil {
		logger.ErrorfWithError(err, "RpcHead: error mounting share for %s", in.VirtPath)
		return err
	}

	entityPath := vContainerName
	if vObjectName != "" {
		entityPath = entityPath + "/" + vObjectName
	}

	resp, err := mountHandle.MiddlewareHeadResponse(entityPath)
	if err != nil {
		if !blunder.Is(err, blunder.NotFoundError) {
			logger.ErrorfWithError(err, "RpcHead: error retrieving metadata for %s", in.VirtPath)
		}
		return err
	}

	reply.Metadata = resp.Metadata
	reply.FileSize = resp.FileSize
	reply.ModificationTime = resp.ModificationTime
	reply.AttrChangeTime = resp.AttrChangeTime
	reply.InodeNumber = int64(uint64(resp.InodeNumber))
	reply.NumWrites = resp.NumWrites

	reply.IsDir = resp.IsDir

	return nil
}

// RpcGetContainer is used by Middleware to issue a GET on a container and return the results.
func (s *Server) RpcGetContainer(in *GetContainerReq, reply *GetContainerReply) (err error) {
	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	_, vContainerName, _, _, mountHandle, err := mountIfNotMounted(in.VirtPath)
	if err != nil {
		logger.ErrorfWithError(err, "RpcGetContainer: error mounting share for %s", in.VirtPath)
		return err
	}

	entries, err := mountHandle.MiddlewareGetContainer(vContainerName, in.MaxEntries, in.Marker, in.Prefix, in.Delimiter)
	if err != nil {
		return err
	}
	resp, err := mountHandle.MiddlewareHeadResponse(vContainerName)
	if err != nil {
		return err
	}
	reply.ContainerEntries = entries
	reply.Metadata = resp.Metadata
	reply.ModificationTime = resp.ModificationTime
	return nil
}

// RpcGetObject is used by GET HTTP request to retrieve the read plan for an object.
func (s *Server) RpcGetObject(in *GetObjectReq, reply *GetObjectReply) (err error) {
	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	_, vContainerName, objectName, volumeName, mountHandle, err := mountIfNotMounted(in.VirtPath)

	mountRelativePath := vContainerName + "/" + objectName

	var ino uint64
	reply.FileSize, reply.ModificationTime, reply.AttrChangeTime, ino, reply.NumWrites, reply.Metadata, err = mountHandle.MiddlewareGetObject(volumeName, mountRelativePath, in.ReadEntsIn, &reply.ReadEntsOut)
	if err != nil {
		return err
	}
	reply.InodeNumber = int64(ino)

	return err
}

// RpcPost handles a POST command from middleware for an account, container or object.
func (s *Server) RpcPost(in *MiddlewarePostReq, reply *MiddlewarePostReply) (err error) {
	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()

	accountName, containerName, objectName, _, mountHandle, err := mountIfNotMounted(in.VirtPath)

	// Don't allow a POST on an invalid account or on just an account
	if accountName == "" || containerName == "" {
		err = fmt.Errorf("%s: Can't modify an account, AccountName: %v is invalid or ContainerName: %v is invalid.", utils.GetFnName(), accountName, containerName)
		logger.ErrorWithError(err)
		err = blunder.AddError(err, blunder.AccountNotModifiable)
		return err
	}

	var parentDir, baseName string
	if objectName != "" {
		parentDir, baseName = splitPath(containerName + "/" + objectName)
	} else {
		parentDir, baseName = splitPath(containerName)
	}

	err = mountHandle.MiddlewarePost(parentDir, baseName, in.NewMetaData, in.OldMetaData)

	return err

}

// Makes a directory. Unlike RpcMkdir, one can invoke this with just a path.
func (s *Server) RpcMiddlewareMkdir(in *MiddlewareMkdirReq, reply *MiddlewareMkdirReply) (err error) {
	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()

	_, containerName, objectName, _, mountHandle, err := mountIfNotMounted(in.VirtPath)

	// Require a reference to an object; you can't create a container with this method.
	if objectName == "" {
		err = blunder.NewError(blunder.NotAnObjectError, "%s: VirtPath must reference an object, not container or account (%s)", utils.GetFnName(), in.VirtPath)
		// This is worth logging; a correct middleware will never send such a path.
		logger.ErrorWithError(err)
		return err
	}

	mtime, ctime, inodeNumber, numWrites, err := mountHandle.MiddlewareMkdir(containerName, objectName, in.Metadata)
	reply.ModificationTime = mtime
	reply.AttrChangeTime = ctime
	reply.InodeNumber = int64(uint64(inodeNumber))
	reply.NumWrites = numWrites
	return
}

// RpcPutComplete is used by PUT HTTP request once data has been put in Swift.
//
// Sets up inode, etc.
func (s *Server) RpcPutComplete(in *PutCompleteReq, reply *PutCompleteReply) (err error) {
	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	_, containerName, objectName, _, mountHandle, err := mountIfNotMounted(in.VirtPath)

	// Call fs to complete the creation of the inode for the file and
	// the directories.
	mtime, ctime, ino, numWrites, err := mountHandle.MiddlewarePutComplete(containerName, objectName, in.PhysPaths, in.PhysLengths, in.Metadata)
	reply.ModificationTime = mtime
	reply.AttrChangeTime = ctime
	reply.InodeNumber = int64(uint64(ino))
	reply.NumWrites = numWrites

	return err
}

// RpcPutLocation is used by PUT HTTP request to provision an object so that middleware
// can PUT the object in Swift.
//
// Later, a RpcPutComplete() will be called to setup inode, etc.
func (s *Server) RpcPutLocation(in *PutLocationReq, reply *PutLocationReply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()

	accountName, containerName, objectName, _, mountHandle, err := mountIfNotMounted(in.VirtPath)

	// Validate the components of the objectName
	err = fs.ValidateFullPath(containerName + "/" + objectName)
	if err != nil {
		return err
	}

	_, baseName := splitPath(containerName + "/" + objectName)
	err = fs.ValidateBaseName(baseName)
	if err != nil {
		return err
	}

	// Via fs package, ask inode package to provision object
	reply.PhysPath, err = mountHandle.CallInodeToProvisionObject()

	if err != nil {
		logger.DebugfIDWithError(internalDebug, err, "fs.CallInodeToProvisionObject() of acct: %v container: %v failed!", accountName, containerName)
	}
	return
}

// RpcPutContainer creates or updates a container (top-level directory).
func (s *Server) RpcPutContainer(in *PutContainerReq, reply *PutContainerReply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	_, containerName, _, _, mountHandle, err := mountIfNotMounted(in.VirtPath)
	if err != nil {
		return err
	}

	err = mountHandle.MiddlewarePutContainer(containerName, in.OldMetadata, in.NewMetadata)
	return err
}

// Combine a bunch of files together into a big one. It's like "cat old1 old2 ... > new", but without the cat. Also
// removes the files old1 old2 ...
func (s *Server) RpcCoalesce(in *CoalesceReq, reply *CoalesceReply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	_, destContainer, destObject, _, mountHandle, err := mountIfNotMounted(in.VirtPath)

	var ino uint64
	ino, reply.NumWrites, reply.ModificationTime, err = mountHandle.MiddlewareCoalesce(destContainer+"/"+destObject, in.ElementAccountRelativePaths)
	reply.InodeNumber = int64(ino)
	return
}

// Renew a lease, ensuring that the related file's log segments won't get deleted. This ensures that an HTTP client is
// able to complete an object GET request regardless of concurrent FS writes or HTTP PUTs to that file.
//
// Middleware calls this periodically while producing an object GET response.
func (s *Server) RpcRenewLease(in *RenewLeaseReq, reply *RenewLeaseReply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	// This is currently a stub, as there's not yet any idea of a lease, so there's nothing to renew.
	return
}

// Release a lease, allowing a file's log segments to be deleted when necessary.
//
// Middleware calls this once an object GET response is complete.
func (s *Server) RpcReleaseLease(in *ReleaseLeaseReq, reply *ReleaseLeaseReply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()
	defer func() { rpcEncodeError(&err) }() // Encode error for return by RPC

	// This is currently a stub, as there's not yet any idea of a lease, so there's nothing to release.
	return
}

// RpcIsAccountBimodal answers the question "is this account known by ProxyFS to be bimodal?".
//
// If bimodal, also indicates the PrivateIPAddr of the Peer ProxyFS instance serving the matching volume
func (s *Server) RpcIsAccountBimodal(in *IsAccountBimodalReq, reply *IsAccountBimodalReply) (err error) {
	var (
		ok         bool
		volumeName string
	)

	globals.gate.RLock()
	defer globals.gate.RUnlock()

	flog := logger.TraceEnter("in.", in)
	defer func() { flog.TraceExitErr("reply.", err, reply) }()

	volumeName, reply.IsBimodal = fs.AccountNameToVolumeName(in.AccountName)

	if reply.IsBimodal {
		reply.ActivePeerPrivateIPAddr, ok = fs.VolumeNameToActivePeerPrivateIPAddr(volumeName)
		if !ok {
			err = fmt.Errorf("%v indicated as Bimodal but no matching ActivePeer", volumeName)
			logger.ErrorWithError(err)
			return
		}
	}

	err = nil
	return
}
