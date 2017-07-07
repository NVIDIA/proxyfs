package inode

func (vS *volumeStruct) GetFSID() (fsid uint64) {
	fsid = vS.fsid
	return
}
