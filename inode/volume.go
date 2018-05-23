package inode

func (vS *volumeStruct) GetFSID() (fsid uint64) {
	fsid = vS.fsid
	return
}

func (vS *volumeStruct) SnapShotCreateByFSLayer(name string) (id uint64, err error) {
	// TODO: Does Inode Layer need to do anything here?
	id, err = vS.headhunterVolumeHandle.SnapShotCreateByInodeLayer(name)
	return
}

func (vS *volumeStruct) SnapShotDeleteByFSLayer(id uint64) (err error) {
	// TODO: Does Inode Layer need to do anything here?
	err = vS.headhunterVolumeHandle.SnapShotDeleteByInodeLayer(id)
	return
}
