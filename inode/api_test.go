package inode

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/swiftstack/ProxyFS/swiftclient"
	"github.com/swiftstack/ProxyFS/utils"
)

const durationToDelayOrSkew = "100ms"

var AllMetadataFields = map[string]bool{
	"InodeType":            true,
	"LinkCount":            true,
	"Size":                 true,
	"CreationTime":         true,
	"ModificationTime":     true,
	"AccessTime":           true,
	"AttrChangeTime":       true,
	"NumWrites":            true,
	"InodeStreamNameSlice": true,
	"Mode":                 true,
	"UserID":               true,
	"GroupID":              true,
}

var MetadataPermFields = map[string]bool{
	"Mode":    true,
	"UserID":  true,
	"GroupID": true,
}

var MetadataLinkCountField = map[string]bool{
	"LinkCount": true,
}

var MetadataCrTimeField = map[string]bool{
	"CreationTime": true,
}

var MetadataModTimeField = map[string]bool{
	"ModificationTime": true,
}

var MetadataSizeField = map[string]bool{
	"Size": true,
}

var MetadataTimeFields = map[string]bool{
	"CreationTime":     true,
	"AttrChangeTime":   true,
	"ModificationTime": true,
	"AccessTime":       true,
}

var MetadataNotAttrTimeFields = map[string]bool{
	"CreationTime":     true,
	"ModificationTime": true,
	"AccessTime":       true,
}

var MetadataNumWritesField = map[string]bool{
	"NumWrites": true,
}

func checkMetadata(t *testing.T, md *MetadataStruct, expMd *MetadataStruct, fieldsToCheck map[string]bool, errorPrefix string) {
	for field := range fieldsToCheck {
		switch field {

		case "InodeType":
			value := md.InodeType
			expValue := expMd.InodeType
			if value != expValue {
				t.Fatalf("%s returned %s %v, expected %v", errorPrefix, field, value, expValue)
			}

		case "LinkCount":
			value := md.LinkCount
			expValue := expMd.LinkCount
			if value != expValue {
				t.Fatalf("%s returned %s %v, expected %v", errorPrefix, field, value, expValue)
			}

		case "Size":
			value := md.Size
			expValue := expMd.Size
			if value != expValue {
				t.Fatalf("%s returned %s %v, expected %v", errorPrefix, field, value, expValue)
			}

		case "CreationTime":
			value := md.CreationTime
			expValue := expMd.CreationTime
			if !value.Equal(expValue) {
				t.Fatalf("%s returned %s %v, expected %v", errorPrefix, field, value, expValue)
			}

		case "ModificationTime":
			value := md.ModificationTime
			expValue := expMd.ModificationTime
			if !value.Equal(expValue) {
				t.Fatalf("%s returned %s %v, expected %v", errorPrefix, field, value, expValue)
			}

		case "AccessTime":
			value := md.AccessTime
			expValue := expMd.AccessTime
			if !value.Equal(expValue) {
				t.Fatalf("%s returned %s %v, expected %v", errorPrefix, field, value, expValue)
			}

		case "AttrChangeTime":
			value := md.AttrChangeTime
			expValue := expMd.AttrChangeTime
			if !value.Equal(expValue) {
				t.Fatalf("%s returned %s %v, expected %v", errorPrefix, field, value, expValue)
			}

		case "NumWrites":
			value := md.NumWrites
			expValue := expMd.NumWrites
			if value != expValue {
				t.Fatalf("%s returned %s %v, expected %v", errorPrefix, field, value, expValue)
			}

		case "InodeStreamNameSlice":
			value := len(md.InodeStreamNameSlice)
			expValue := len(expMd.InodeStreamNameSlice)
			if value != expValue {
				t.Fatalf("%s returned %s %v, expected %v", errorPrefix, field, value, expValue)
			}
			for i, streamName := range md.InodeStreamNameSlice {
				value := streamName
				expValue := expMd.InodeStreamNameSlice[i]
				if value != expValue {
					t.Fatalf("%s returned %s[%d] %v, expected %v", errorPrefix,
						field, i, value, expValue)
				}
			}

		case "Mode":
			value := md.Mode
			expValue := expMd.Mode

			// Add the file type to the expected mode
			if md.InodeType == DirType {
				expValue |= PosixModeDir
			} else if md.InodeType == SymlinkType {
				expValue |= PosixModeSymlink
			} else if md.InodeType == FileType {
				expValue |= PosixModeFile
			}
			if value != expValue {
				t.Fatalf("%s returned %s %v, expected %v", errorPrefix, field, value, expValue)
			}

		case "UserID":
			value := md.UserID
			expValue := expMd.UserID
			if value != expValue {
				t.Fatalf("%s returned %s %v, expected %v", errorPrefix, field, value, expValue)
			}

		case "GroupID":
			value := md.GroupID
			expValue := expMd.GroupID
			if value != expValue {
				t.Fatalf("%s returned %s %v, expected %v", errorPrefix, field, value, expValue)
			}

		default:
			// catch field name typos
			t.Fatalf("%s specified unknown field '%v'", errorPrefix, field)
		}
	}
}

func checkMetadataTimeChanges(t *testing.T, oldMd *MetadataStruct, newMd *MetadataStruct, creationTimeShouldChange bool, modificationTimeShouldChange bool, accessTimeShouldChange bool, attrChangeTimeShouldChange bool, errorPrefix string) {
	if creationTimeShouldChange {
		if oldMd.CreationTime == newMd.CreationTime {
			t.Fatalf("%s should have changed CreationTime", errorPrefix)
		}
	} else {
		if oldMd.CreationTime != newMd.CreationTime {
			t.Fatalf("%s should not have changed CreationTime", errorPrefix)
		}
	}

	if modificationTimeShouldChange {
		if oldMd.ModificationTime == newMd.ModificationTime {
			t.Fatalf("%s should have changed ModificationTime", errorPrefix)
		}
	} else {
		if oldMd.ModificationTime != newMd.ModificationTime {
			t.Fatalf("%s should not have changed ModificationTime", errorPrefix)
		}
	}

	if accessTimeShouldChange {
		if oldMd.AccessTime == newMd.AccessTime {
			t.Fatalf("%s should have changed AccessTime", errorPrefix)
		}
	} else {
		if oldMd.AccessTime != newMd.AccessTime {
			t.Fatalf("%s should not have changed AccessTime", errorPrefix)
		}
	}

	if attrChangeTimeShouldChange {
		if oldMd.AttrChangeTime == newMd.AttrChangeTime {
			t.Fatalf("%s should have changed AttrChangeTime", errorPrefix)
		}
	} else {
		if oldMd.AttrChangeTime != newMd.AttrChangeTime {
			t.Fatalf("%s should not have changed AttrChangeTime", errorPrefix)
		}
	}
}

func TestAPI(t *testing.T) {
	var (
		timeBeforeOp time.Time
		timeAfterOp  time.Time
	)

	testSetup(t, false)

	_, ok := AccountNameToVolumeName("BadAccountName")
	if ok {
		t.Fatalf("AccountNameToVolumeName(\"BadAccountName\") should have failed")
	}

	goodVolumeName, ok := AccountNameToVolumeName("AUTH_test")
	if !ok {
		t.Fatalf("AccountNameToVolumeName(\"AUTH_test\") should have succeeded")
	}
	if "TestVolume" != goodVolumeName {
		t.Fatalf("AccountNameToVolumeName(\"AUTH_test\") should have returned \"TestVolume\"")
	}

	_, ok = VolumeNameToActivePeerPrivateIPAddr("BadVolumeName")
	if ok {
		t.Fatalf("VolumeNameToActivePeerPrivateIPAddr(\"BadVolumeName\") should have failed")
	}

	goodActivePeerPrivateIPAddr, ok := VolumeNameToActivePeerPrivateIPAddr("TestVolume")
	if !ok {
		t.Fatalf("VolumeNameToActivePeerPrivateIPAddr(\"TestVolume\") should have succeeded")
	}
	if "127.0.0.1" != goodActivePeerPrivateIPAddr {
		t.Fatalf("VolumeNameToActivePeerPrivateIPAddr(\"TestVolume\") should have returned \"127.0.0.1\"")
	}

	_, err := FetchVolumeHandle("BadVolumeName")
	if nil == err {
		t.Fatalf("FetchVolumeHandle(\"BadVolumeName\") should have failed")
	}

	testVolumeHandle, err := FetchVolumeHandle("TestVolume")
	if nil != err {
		t.Fatalf("FetchVolumeHandle(\"TestVolume\") should have worked - got error: %v", err)
	}

	fsid := testVolumeHandle.GetFSID()
	if 1 != fsid {
		t.Fatalf("GetFSID() returned unexpected FSID")
	}

	fileInodeNumber, err := testVolumeHandle.CreateFile(InodeMode(0000), InodeRootUserID, InodeGroupID(0))
	if nil != err {
		t.Fatalf("CreateFile() failed: %v", err)
	}

	if !testVolumeHandle.Access(fileInodeNumber, InodeRootUserID, InodeGroupID(0), nil, F_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,,,,F_OK) after CreateFile() should not have failed")
	}

	if !testVolumeHandle.Access(fileInodeNumber, InodeRootUserID, InodeGroupID(456), nil,
		R_OK|W_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,InodeUserID(123),,,R_OK|W_OK) should have returned true")
	}

	// not even root can execute a file without any execute bits set
	if testVolumeHandle.Access(fileInodeNumber, InodeRootUserID, InodeGroupID(456), nil,
		X_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,InodeUserID(123),,,R_OK|W_OK|X_OK) should have returned false")
	}

	if testVolumeHandle.Access(fileInodeNumber, InodeUserID(123), InodeGroupID(0), nil,
		R_OK|W_OK|X_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,InodeUserID(123),InodeGroupID(0),,R_OK|W_OK|X_OK) should have returned false")
	}

	if testVolumeHandle.Access(fileInodeNumber, InodeRootUserID, InodeGroupID(0), nil,
		R_OK|P_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,,,,R_OK|X_OK) should have returned false")
	}

	err = testVolumeHandle.SetOwnerUserID(fileInodeNumber, InodeUserID(123))
	if nil != err {
		t.Fatalf("SetOwnerUserID(,InodeUserID(123)) failed: %v", err)
	}

	if !testVolumeHandle.Access(fileInodeNumber, InodeRootUserID, InodeGroupID(0), nil, P_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,InodeRootUserID,,,P_OK should have returned true")
	}

	if !testVolumeHandle.Access(fileInodeNumber, InodeUserID(123), InodeGroupID(0), nil, P_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,InodeUserID(123),,,P_OK should have returned true")
	}

	if !testVolumeHandle.Access(fileInodeNumber, InodeUserID(123), InodeGroupID(0), nil, P_OK, OwnerOverride) {
		t.Fatalf("Access(fileInodeNumber,InodeUserID(123),,,P_OK,UserOveride should have returned true")
	}

	if testVolumeHandle.Access(fileInodeNumber, InodeUserID(789), InodeGroupID(0), nil, P_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,InodeUserID(789),,,P_OK should have returned false")
	}

	err = testVolumeHandle.SetPermMode(fileInodeNumber, InodeMode(0600))
	if nil != err {
		t.Fatalf("SetPermMode(,InodeMode(0600)) failed: %v", err)
	}

	if !testVolumeHandle.Access(fileInodeNumber, InodeUserID(123), InodeGroupID(456), nil, R_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,InodeUserID(123),,,R_OK) should have returned true")
	}

	if !testVolumeHandle.Access(fileInodeNumber, InodeUserID(123), InodeGroupID(456), nil, W_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,InodeUserID(123),,,W_OK) should have returned true")
	}

	if testVolumeHandle.Access(fileInodeNumber, InodeUserID(123), InodeGroupID(456), nil, X_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,InodeUserID(123),,,X_OK) should have returned false")
	}

	if !testVolumeHandle.Access(fileInodeNumber, InodeUserID(123), InodeGroupID(456), nil, R_OK|W_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,InodeUserID(123),,,R_OK|W_OK) should have returned true")
	}

	if testVolumeHandle.Access(fileInodeNumber, InodeUserID(123), InodeGroupID(456), nil,
		R_OK|W_OK|X_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,InodeUserID(123),,,R_OK|W_OK|X_OK) should have returned false")
	}

	if testVolumeHandle.Access(fileInodeNumber, InodeUserID(123), InodeGroupID(456), nil,
		R_OK|W_OK|X_OK, OwnerOverride) {
		t.Fatalf("Access(fileInodeNumber,InodeUserID(123),OwnerOverride,,R_OK|W_OK|X_OK) should have returned false")
	}

	err = testVolumeHandle.SetOwnerUserIDGroupID(fileInodeNumber, InodeRootUserID, InodeGroupID(456))
	if nil != err {
		t.Fatalf("SetOwnerUserIDGroupID(,InodeRootUserID,InodeGroupID(456)) failed: %v", err)
	}

	err = testVolumeHandle.SetPermMode(fileInodeNumber, InodeMode(0060))
	if nil != err {
		t.Fatalf("SetPermMode(,InodeMode(0060)) failed: %v", err)
	}

	if !testVolumeHandle.Access(fileInodeNumber, InodeUserID(123), InodeGroupID(456), nil, R_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,,InodeGroupID(456),,R_OK) should have returned true")
	}

	if !testVolumeHandle.Access(fileInodeNumber, InodeUserID(123), InodeGroupID(456), nil, W_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,,InodeGroupID(456),,W_OK) should have returned true")
	}

	if testVolumeHandle.Access(fileInodeNumber, InodeUserID(123), InodeGroupID(456), nil, X_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,,InodeGroupID(456),,X_OK) should have returned false")
	}

	if testVolumeHandle.Access(fileInodeNumber, InodeUserID(123), InodeGroupID(456), nil, X_OK, OwnerOverride) {
		t.Fatalf("Access(fileInodeNumber,,InodeGroupID(456),,X_OK,OwnerOverride) should have returned false")
	}

	if !testVolumeHandle.Access(fileInodeNumber, InodeUserID(123), InodeGroupID(456), nil,
		R_OK|W_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,,InodeGroupID(456),,R_OK|W_OK) should have returned true")
	}

	if testVolumeHandle.Access(fileInodeNumber, InodeUserID(123), InodeGroupID(456), nil,
		R_OK|W_OK|X_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,,InodeGroupID(456),,R_OK|W_OK|X_OK) should have returned false")
	}

	if !testVolumeHandle.Access(fileInodeNumber, InodeUserID(123), InodeGroupID(789),
		[]InodeGroupID{InodeGroupID(456)}, R_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,,,[]InodeGroupID{InodeGroupID(456)},R_OK) should have returned true")
	}

	if !testVolumeHandle.Access(fileInodeNumber, InodeUserID(123), InodeGroupID(789),
		[]InodeGroupID{InodeGroupID(456)}, W_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,,,[]InodeGroupID{InodeGroupID(456)},W_OK) should have returned true")
	}

	if testVolumeHandle.Access(fileInodeNumber, InodeUserID(123), InodeGroupID(789),
		[]InodeGroupID{InodeGroupID(456)}, X_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,,,[]InodeGroupID{InodeGroupID(456)},X_OK) should have returned false")
	}

	if !testVolumeHandle.Access(fileInodeNumber, InodeUserID(123), InodeGroupID(789),
		[]InodeGroupID{InodeGroupID(456)}, R_OK|W_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,,,[]InodeGroupID{InodeGroupID(456)},R_OK|W_OK) should have returned true")
	}

	if testVolumeHandle.Access(fileInodeNumber, InodeUserID(123), InodeGroupID(789),
		[]InodeGroupID{InodeGroupID(456)}, R_OK|W_OK|X_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,,,[]InodeGroupID{InodeGroupID(456)},R_OK|W_OK|X_OK) should have returned false")
	}

	err = testVolumeHandle.SetPermMode(fileInodeNumber, InodeMode(0006))
	if nil != err {
		t.Fatalf("SetPermMode(,InodeMode(0006)) failed: %v", err)
	}
	err = testVolumeHandle.SetOwnerUserIDGroupID(fileInodeNumber, InodeUserID(456), InodeGroupID(0))
	if nil != err {
		t.Fatalf("SetOwnerUserIDGroupID(,InodeRootUserID,InodeGroupID(0)) failed: %v", err)
	}

	if !testVolumeHandle.Access(fileInodeNumber, InodeUserID(123), InodeGroupID(456), nil, R_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,,,,R_OK) should have returned true")
	}

	if !testVolumeHandle.Access(fileInodeNumber, InodeUserID(123), InodeGroupID(456), nil, W_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,,,,W_OK) should have returned true")
	}

	if testVolumeHandle.Access(fileInodeNumber, InodeUserID(123), InodeGroupID(456), nil, X_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,,,,X_OK) should have returned false")
	}

	if !testVolumeHandle.Access(fileInodeNumber, InodeUserID(123), InodeGroupID(456), nil,
		R_OK|W_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,,,,R_OK|W_OK) should have returned true")
	}

	if testVolumeHandle.Access(fileInodeNumber, InodeUserID(123), InodeGroupID(456), nil,
		R_OK|W_OK|X_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,,,,R_OK|W_OK|X_OK) should have returned false")
	}

	// Test the ability of OwnerOverride to override permissions checks for owner (except for exec)
	if testVolumeHandle.Access(fileInodeNumber, InodeUserID(456), InodeGroupID(456), nil, R_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,,,,R_OK) should have returned false")
	}
	if !testVolumeHandle.Access(fileInodeNumber, InodeUserID(456), InodeGroupID(456), nil, R_OK, OwnerOverride) {
		t.Fatalf("Access(fileInodeNumber,,,,R_OK) should have returned true")
	}

	if testVolumeHandle.Access(fileInodeNumber, InodeUserID(456), InodeGroupID(456), nil, W_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,,,,W_OK) should have returned false")
	}
	if !testVolumeHandle.Access(fileInodeNumber, InodeUserID(456), InodeGroupID(456), nil, W_OK, OwnerOverride) {
		t.Fatalf("Access(fileInodeNumber,,,,W_OK) should have returned true")
	}

	if testVolumeHandle.Access(fileInodeNumber, InodeUserID(456), InodeGroupID(456), nil, X_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,,,,X_OK) should have returned false")
	}
	if testVolumeHandle.Access(fileInodeNumber, InodeUserID(456), InodeGroupID(456), nil, X_OK, OwnerOverride) {
		t.Fatalf("Access(fileInodeNumber,,,,X_OK) should have returned false")
	}

	err = testVolumeHandle.Destroy(fileInodeNumber)
	if nil != err {
		t.Fatalf("Destroy(fileInodeNumber) failed: %v", err)
	}

	if testVolumeHandle.Access(fileInodeNumber, InodeRootUserID, InodeGroupID(0), nil, F_OK, NoOverride) {
		t.Fatalf("Access(fileInodeNumber,,,,F_OK) after Destroy() should have failed")
	}

	rootDirInodeType, err := testVolumeHandle.GetType(RootDirInodeNumber)
	if nil != err {
		t.Fatalf("GetType(RootDirInodeNumber) failed: %v", err)
	}
	if DirType != rootDirInodeType {
		t.Fatalf("GetType(RootDirInodeNumber) returned unexpected type: %v", rootDirInodeType)
	}

	rootDirLinkCount, err := testVolumeHandle.GetLinkCount(RootDirInodeNumber)
	if nil != err {
		t.Fatalf("GetLinkCount(RootDirInodeNumber) failed: %v", err)
	}
	if 2 != rootDirLinkCount {
		t.Fatalf("GetLinkCount(RootDirInodeNumber) returned unexpected linkCount: %v", rootDirLinkCount)
	}

	numEntries, err := testVolumeHandle.NumDirEntries(RootDirInodeNumber)

	if nil != err {
		t.Fatalf("NumDirEntries(RootDirInodeNumber) failed: %v", err)
	}
	if 2 != numEntries {
		t.Fatalf("NumDirEntries(RootDirInodeNumber) should have returned numEntries == 2")
	}

	dirEntrySlice, moreEntries, err := testVolumeHandle.ReadDir(RootDirInodeNumber, 0, 0)

	if nil != err {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) failed: %v", err)
	}
	if moreEntries {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) should have returned moreEntries == false")
	}
	if 2 != len(dirEntrySlice) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) should have returned dirEntrySlice with 2 elements")
	}
	if (dirEntrySlice[0].InodeNumber != RootDirInodeNumber) || (dirEntrySlice[0].Basename != ".") || (dirEntrySlice[0].NextDirLocation != 1) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) returned unexpected dirEntrySlice[0]")
	}
	if (dirEntrySlice[1].InodeNumber != RootDirInodeNumber) || (dirEntrySlice[1].Basename != "..") || (dirEntrySlice[1].NextDirLocation != 2) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) returned unexpected dirEntrySlice[1]")
	}

	dirEntrySlice, moreEntries, err = testVolumeHandle.ReadDir(RootDirInodeNumber, 0, 0, InodeDirLocation(-1))

	if nil != err {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0, InodeDirLocation(-1)) failed: %v", err)
	}
	if moreEntries {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0, InodeDirLocation(-1)) should have returned moreEntries == false")
	}
	if 2 != len(dirEntrySlice) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0, InodeDirLocation(-1)) should have returned dirEntrySlice with 2 elements")
	}
	if (dirEntrySlice[0].InodeNumber != RootDirInodeNumber) || (dirEntrySlice[0].Basename != ".") || (dirEntrySlice[0].NextDirLocation != 1) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0, InodeDirLocation(-1)) returned unexpected dirEntrySlice[0]")
	}
	if (dirEntrySlice[1].InodeNumber != RootDirInodeNumber) || (dirEntrySlice[1].Basename != "..") || (dirEntrySlice[1].NextDirLocation != 2) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0, InodeDirLocation(-1)) returned unexpected dirEntrySlice[1]")
	}

	dirEntrySlice, moreEntries, err = testVolumeHandle.ReadDir(RootDirInodeNumber, 0, 0, "")

	if nil != err {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0, \"\") failed: %v", err)
	}
	if moreEntries {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0, \"\") should have returned moreEntries == false")
	}
	if 2 != len(dirEntrySlice) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0, \"\") should have returned dirEntrySlice with 2 elements")
	}
	if (dirEntrySlice[0].InodeNumber != RootDirInodeNumber) || (dirEntrySlice[0].Basename != ".") || (dirEntrySlice[0].NextDirLocation != 1) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0, \"\") returned unexpected dirEntrySlice[0]")
	}
	if (dirEntrySlice[1].InodeNumber != RootDirInodeNumber) || (dirEntrySlice[1].Basename != "..") || (dirEntrySlice[1].NextDirLocation != 2) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0, \"\") returned unexpected dirEntrySlice[1]")
	}

	testMetadata := &MetadataStruct{
		InodeType: FileType,
		NumWrites: 0,
		LinkCount: 0,
		Mode:      PosixModePerm,
		UserID:    InodeUserID(123),
		GroupID:   InodeGroupID(456),
	}

	fileInodeNumber, err = testVolumeHandle.CreateFile(testMetadata.Mode, testMetadata.UserID, testMetadata.GroupID)
	if nil != err {
		t.Fatalf("CreateFile() failed: %v", err)
	}

	fileInodeType, err := testVolumeHandle.GetType(fileInodeNumber)
	if nil != err {
		t.Fatalf("GetType(fileInodeNumber) failed: %v", err)
	}
	if FileType != fileInodeType {
		t.Fatalf("GetType(fileInodeNumber) returned unexpected type: %v", fileInodeType)
	}

	fileLinkCount, err := testVolumeHandle.GetLinkCount(fileInodeNumber)
	if nil != err {
		t.Fatalf("GetLinkCount(fileInodeNumber) failed: %v", err)
	}
	if 0 != fileLinkCount {
		t.Fatalf("GetLinkCount(fileInodeNumber) returned unexpected linkCount: %v", fileLinkCount)
	}

	postMetadata, err := testVolumeHandle.GetMetadata(fileInodeNumber)
	if nil != err {
		t.Fatalf("GetMetadata(fileInodeNumber) failed: %v", err)
	}
	if FileType != postMetadata.InodeType {
		t.Fatalf("GetMetadata(fileInodeNumber) returned unexpected InodeType")
	}
	checkMetadata(t, postMetadata, testMetadata, MetadataLinkCountField, "GetMetadata() after CreateFile()")

	// XXX TODO: Add more tests related to CreateFile(): mode > 0777, etc.

	preMetadata, err := testVolumeHandle.GetMetadata(fileInodeNumber)
	if nil != err {
		t.Fatalf("GetMetadata(fileInodeNumber) failed: %v", err)
	}
	time.Sleep(time.Millisecond)

	testMetadata.Mode = PosixModePerm - 1

	err = testVolumeHandle.SetPermMode(fileInodeNumber, testMetadata.Mode)
	if nil != err {
		t.Fatalf("SetPermMode(fileInodeNumber, %v) failed: %v", testMetadata.Mode, err)
	}

	postMetadata, err = testVolumeHandle.GetMetadata(fileInodeNumber)
	if nil != err {
		t.Fatalf("GetMetadata(fileInodeNumber) failed: %v", err)
	}
	checkMetadata(t, postMetadata, testMetadata, MetadataPermFields, "GetMetadata() after SetPermMode()")
	checkMetadataTimeChanges(t, preMetadata, postMetadata, false, false, false, true, "SetPermMode()")

	preMetadata, err = testVolumeHandle.GetMetadata(fileInodeNumber)
	if nil != err {
		t.Fatalf("GetMetadata(fileInodeNumber) failed: %v", err)
	}
	time.Sleep(time.Millisecond)

	testMetadata.UserID = 9753

	err = testVolumeHandle.SetOwnerUserID(fileInodeNumber, testMetadata.UserID)
	if nil != err {
		t.Fatalf("SetOwnerUserID(fileInodeNumber, %v) failed: %v", testMetadata.UserID, err)
	}

	postMetadata, err = testVolumeHandle.GetMetadata(fileInodeNumber)
	if nil != err {
		t.Fatalf("GetMetadata(fileInodeNumber) failed: %v", err)
	}
	checkMetadata(t, postMetadata, testMetadata, MetadataPermFields, "GetMetadata() after SetOwnerUserID()")
	checkMetadataTimeChanges(t, preMetadata, postMetadata, false, false, false, true, "SetOwnerUserID()")

	preMetadata, err = testVolumeHandle.GetMetadata(fileInodeNumber)
	if nil != err {
		t.Fatalf("GetMetadata(fileInodeNumber) failed: %v", err)
	}
	time.Sleep(time.Millisecond)

	testMetadata.GroupID = 12345678

	err = testVolumeHandle.SetOwnerGroupID(fileInodeNumber, testMetadata.GroupID)
	if nil != err {
		t.Fatalf("SetOwnerGroupID(fileInodeNumber, %v) failed: %v", testMetadata.GroupID, err)
	}

	postMetadata, err = testVolumeHandle.GetMetadata(fileInodeNumber)
	if nil != err {
		t.Fatalf("GetMetadata(fileInodeNumber) failed: %v", err)
	}
	checkMetadata(t, postMetadata, testMetadata, MetadataPermFields, "GetMetadata() after SetOwnerGroupID()")
	checkMetadataTimeChanges(t, preMetadata, postMetadata, false, false, false, true, "SetOwnerGroupID()")

	preMetadata, err = testVolumeHandle.GetMetadata(fileInodeNumber)
	if nil != err {
		t.Fatalf("GetMetadata(fileInodeNumber) failed: %v", err)
	}
	time.Sleep(time.Millisecond)

	testMetadata.UserID = 3579
	testMetadata.GroupID = 87654321

	err = testVolumeHandle.SetOwnerUserIDGroupID(fileInodeNumber, testMetadata.UserID, testMetadata.GroupID)
	if nil != err {
		t.Fatalf("SetOwnerUserIDGroupID(fileInodeNumber, %v, %v) failed: %v", testMetadata.UserID, testMetadata.GroupID, err)
	}

	postMetadata, err = testVolumeHandle.GetMetadata(fileInodeNumber)
	if nil != err {
		t.Fatalf("GetMetadata(fileInodeNumber) failed: %v", err)
	}
	checkMetadata(t, postMetadata, testMetadata, MetadataPermFields, "GetMetadata() after SetOwnerUserIDGroupID()")
	checkMetadataTimeChanges(t, preMetadata, postMetadata, false, false, false, true, "SetOwnerUserIDGroupID()")

	err = testVolumeHandle.Link(RootDirInodeNumber, "link_1_to_file_inode", fileInodeNumber, false)
	if nil != err {
		t.Fatalf("Link(RootDirInodeNumber, \"link_1_to_file_inode\", fileInodeNumber, false) failed: %v", err)
	}

	numEntries, err = testVolumeHandle.NumDirEntries(RootDirInodeNumber)

	if nil != err {
		t.Fatalf("NumDirEntries(RootDirInodeNumber) failed: %v", err)
	}
	if 3 != numEntries {
		t.Fatalf("NumDirEntries(RootDirInodeNumber) should have returned numEntries == 3")
	}

	fileLinkCount, err = testVolumeHandle.GetLinkCount(fileInodeNumber)
	if nil != err {
		t.Fatalf("GetLinkCount(fileInodeNumber) failed: %v", err)
	}
	if 1 != fileLinkCount {
		t.Fatalf("GetLinkCount(fileInodeNumber) returned unexpected linkCount: %v", fileLinkCount)
	}

	postMetadata, err = testVolumeHandle.GetMetadata(fileInodeNumber)
	if nil != err {
		t.Fatalf("GetMetadata(fileInodeNumber) failed: %v", err)
	}
	testMetadata.LinkCount = 1
	checkMetadata(t, postMetadata, testMetadata, MetadataLinkCountField, "GetMetadata() after Link()")

	err = testVolumeHandle.Link(RootDirInodeNumber, "link_2_to_file_inode", fileInodeNumber, false)
	if nil != err {
		t.Fatalf("Link(RootDirInodeNumber, \"link_2_to_file_inode\", fileInodeNumber, false) failed: %v", err)
	}

	numEntries, err = testVolumeHandle.NumDirEntries(RootDirInodeNumber)

	if nil != err {
		t.Fatalf("NumDirEntries(RootDirInodeNumber) failed: %v", err)
	}
	if 4 != numEntries {
		t.Fatalf("NumDirEntries(RootDirInodeNumber) should have returned numEntries == 4")
	}

	fileLinkCount, err = testVolumeHandle.GetLinkCount(fileInodeNumber)
	if nil != err {
		t.Fatalf("GetLinkCount(fileInodeNumber) failed: %v", err)
	}
	if 2 != fileLinkCount {
		t.Fatalf("GetLinkCount(fileInodeNumber) returned unexpected linkCount: %v", fileLinkCount)
	}

	postMetadata, err = testVolumeHandle.GetMetadata(fileInodeNumber)
	if nil != err {
		t.Fatalf("GetMetadata(fileInodeNumber) failed: %v", err)
	}
	testMetadata.LinkCount = 2
	checkMetadata(t, postMetadata, testMetadata, MetadataLinkCountField, "GetMetadata() after Link()")

	symlinkInodeToLink1ToFileInode, err := testVolumeHandle.CreateSymlink("link_1_to_file_inode", PosixModePerm, 0, 0)
	if nil != err {
		t.Fatalf("CreateSymlink(\"link_1_to_file_inode\") failed: %v", err)
	}

	symlinkInodeToLink1ToFileInodeType, err := testVolumeHandle.GetType(symlinkInodeToLink1ToFileInode)
	if nil != err {
		t.Fatalf("GetType(symlinkInodeToLink1ToFileInode) failed: %v", err)
	}
	if SymlinkType != symlinkInodeToLink1ToFileInodeType {
		t.Fatalf("GetType(symlinkInodeToLink1ToFileInode) returned unexpected type: %v", symlinkInodeToLink1ToFileInodeType)
	}

	symlinkToLink1ToFileInodeLinkCount, err := testVolumeHandle.GetLinkCount(symlinkInodeToLink1ToFileInode)
	if nil != err {
		t.Fatalf("GetLinkCount(symlinkInodeToLink1ToFileInode) failed: %v", err)
	}
	if 0 != symlinkToLink1ToFileInodeLinkCount {
		t.Fatalf("GetLinkCount(symlinkInodeToLink1ToFileInode) returned unexpected linkCount: %v", symlinkToLink1ToFileInodeLinkCount)
	}

	symlinkInodeToLink1ToFileInodeTarget, err := testVolumeHandle.GetSymlink(symlinkInodeToLink1ToFileInode)
	if nil != err {
		t.Fatalf("GetSymlink(symlinkInodeToLink1ToFileInode) failed: %v", err)
	}
	if "link_1_to_file_inode" != symlinkInodeToLink1ToFileInodeTarget {
		t.Fatalf("GetSymlink(symlinkInodeToLink1ToFileInode) should have returned target == \"%v\"... instead got \"%v\"", "link_1_to_file_inode", symlinkInodeToLink1ToFileInodeTarget)
	}

	err = testVolumeHandle.Link(RootDirInodeNumber, "symlink_to_link_1_to_file_inode", symlinkInodeToLink1ToFileInode, false)
	if nil != err {
		t.Fatalf("Link(RootDirInodeNumber, \"symlink_to_link_1_to_file_inode\", symlinkInodeToLink1ToFileInode, false) failed: %v", err)
	}

	numEntries, err = testVolumeHandle.NumDirEntries(RootDirInodeNumber)

	if nil != err {
		t.Fatalf("NumDirEntries(RootDirInodeNumber) failed: %v", err)
	}
	if 5 != numEntries {
		t.Fatalf("NumDirEntries(RootDirInodeNumber) should have returned numEntries == 5")
	}

	symlinkToLink1ToFileInodeLinkCount, err = testVolumeHandle.GetLinkCount(symlinkInodeToLink1ToFileInode)
	if nil != err {
		t.Fatalf("GetLinkCount(symlinkInodeToLink1ToFileInode) failed: %v", err)
	}
	if 1 != symlinkToLink1ToFileInodeLinkCount {
		t.Fatalf("GetLinkCount(symlinkInodeToLink1ToFileInode) returned unexpected linkCount: %v", symlinkToLink1ToFileInodeLinkCount)
	}

	symlinkInodeToLink1ToFileInodeLookupResult, err := testVolumeHandle.Lookup(RootDirInodeNumber, "symlink_to_link_1_to_file_inode")
	if nil != err {
		t.Fatalf("Lookup(RootDirInodeNumber, \"symlink_to_link_1_to_file_inode\") failed: %v", err)
	}
	if symlinkInodeToLink1ToFileInodeLookupResult != symlinkInodeToLink1ToFileInode {
		t.Fatalf("Lookup(RootDirInodeNumber, \"symlink_to_link_1_to_file_inode\") returned unexpected InodeNumber")
	}

	dirEntrySlice, moreEntries, err = testVolumeHandle.ReadDir(RootDirInodeNumber, 1, 0)
	if nil != err {
		t.Fatalf("ReadDir(RootDirInodeNumber, 1, 0) failed: %v", err)
	}
	if !moreEntries {
		t.Fatalf("ReadDir(RootDirInodeNumber, 1, 0) should have returned moreEntries == true")
	}
	if 1 != len(dirEntrySlice) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 1, 0) should have returned dirEntrySlice with 1 element, got %v elements", len(dirEntrySlice))
	}
	if (dirEntrySlice[0].InodeNumber != RootDirInodeNumber) || (dirEntrySlice[0].Basename != ".") || (dirEntrySlice[0].NextDirLocation != 1) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 1, 0) returned unexpected dirEntrySlice[0]")
	}

	// 60 = ".." entry length (8 + (2 + 1) + 2 + 8) + "link_1_to_file_inode" entry length (8 + (20 + 1) + 2 + 8)
	dirEntrySlice, moreEntries, err = testVolumeHandle.ReadDir(RootDirInodeNumber, 0, 60, InodeDirLocation(0))
	if nil != err {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 60, InodeDirLocation(0)) failed: %v", err)
	}
	if !moreEntries {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 60, InodeDirLocation(0)) should have returned moreEntries == true")
	}
	if 2 != len(dirEntrySlice) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 60, InodeDirLocation(0)) should have returned dirEntrySlice with 2 elements, got %v elements", len(dirEntrySlice))
	}
	if (dirEntrySlice[0].InodeNumber != RootDirInodeNumber) || (dirEntrySlice[0].Basename != "..") || (dirEntrySlice[0].NextDirLocation != 2) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 60, InodeDirLocation(0)) returned unexpected dirEntrySlice[0]")
	}
	if (dirEntrySlice[1].InodeNumber != fileInodeNumber) || (dirEntrySlice[1].Basename != "link_1_to_file_inode") || (dirEntrySlice[1].NextDirLocation != 3) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 60, InodeDirLocation(0)) returned unexpected dirEntrySlice[1]")
	}

	dirEntrySlice, moreEntries, err = testVolumeHandle.ReadDir(RootDirInodeNumber, 0, 60, ".")
	if nil != err {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 60, \".\") failed: %v", err)
	}
	if !moreEntries {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 60, \".\") should have returned moreEntries == true")
	}
	if 2 != len(dirEntrySlice) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 60, \".\") should have returned dirEntrySlice with 2 elements, got %v elements", len(dirEntrySlice))
	}
	if (dirEntrySlice[0].InodeNumber != RootDirInodeNumber) || (dirEntrySlice[0].Basename != "..") || (dirEntrySlice[0].NextDirLocation != 2) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 60, \".\") returned unexpected dirEntrySlice[0]")
	}
	if (dirEntrySlice[1].InodeNumber != fileInodeNumber) || (dirEntrySlice[1].Basename != "link_1_to_file_inode") || (dirEntrySlice[1].NextDirLocation != 3) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 60, \".\") returned unexpected dirEntrySlice[1]")
	}

	dirEntrySlice, moreEntries, err = testVolumeHandle.ReadDir(RootDirInodeNumber, 0, 0, InodeDirLocation(2))
	if nil != err {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0, InodeDirLocation(2)) failed: %v", err)
	}
	if moreEntries {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0, InodeDirLocation(2)) should have returned moreEntries == false")
	}
	if 2 != len(dirEntrySlice) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0, InodeDirLocation(2)) should have returned dirEntrySlice with 2 elements")
	}
	if (dirEntrySlice[0].InodeNumber != fileInodeNumber) || (dirEntrySlice[0].Basename != "link_2_to_file_inode") || (dirEntrySlice[0].NextDirLocation != 4) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0, InodeDirLocation(2)) returned unexpected dirEntrySlice[0]")
	}
	if (dirEntrySlice[1].InodeNumber != symlinkInodeToLink1ToFileInode) || (dirEntrySlice[1].Basename != "symlink_to_link_1_to_file_inode") || (dirEntrySlice[1].NextDirLocation != 5) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0, InodeDirLocation(2)) returned unexpected dirEntrySlice[1]")
	}

	dirEntrySlice, moreEntries, err = testVolumeHandle.ReadDir(RootDirInodeNumber, 0, 0, "link_1_to_file_inode")
	if nil != err {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0, \"link_1_to_file_inode\") failed: %v", err)
	}
	if moreEntries {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0, \"link_1_to_file_inode\") should have returned moreEntries == false")
	}
	if 2 != len(dirEntrySlice) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0, \"link_1_to_file_inode\") should have returned dirEntrySlice with 2 elements")
	}
	if (dirEntrySlice[0].InodeNumber != fileInodeNumber) || (dirEntrySlice[0].Basename != "link_2_to_file_inode") || (dirEntrySlice[0].NextDirLocation != 4) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0, \"link_1_to_file_inode\") returned unexpected dirEntrySlice[0]")
	}
	if (dirEntrySlice[1].InodeNumber != symlinkInodeToLink1ToFileInode) || (dirEntrySlice[1].Basename != "symlink_to_link_1_to_file_inode") || (dirEntrySlice[1].NextDirLocation != 5) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0, \"link_1_to_file_inode\") returned unexpected dirEntrySlice[1]")
	}

	err = testVolumeHandle.Unlink(RootDirInodeNumber, "link_1_to_file_inode", false)
	if nil != err {
		t.Fatalf("Unlink(RootDirInodeNumber, \"link_1_to_file_inode\", false) failed: %v", err)
	}

	numEntries, err = testVolumeHandle.NumDirEntries(RootDirInodeNumber)

	if nil != err {
		t.Fatalf("NumDirEntries(RootDirInodeNumber) failed: %v", err)
	}
	if 4 != numEntries {
		t.Fatalf("NumDirEntries(RootDirInodeNumber) should have returned numEntries == 4")
	}

	err = testVolumeHandle.Unlink(RootDirInodeNumber, "link_2_to_file_inode", false)
	if nil != err {
		t.Fatalf("Unlink(RootDirInodeNumber, \"link_2_to_file_inode\", false) failed: %v", err)
	}

	numEntries, err = testVolumeHandle.NumDirEntries(RootDirInodeNumber)

	if nil != err {
		t.Fatalf("NumDirEntries(RootDirInodeNumber) failed: %v", err)
	}
	if 3 != numEntries {
		t.Fatalf("NumDirEntries(RootDirInodeNumber) should have returned numEntries == 3")
	}

	err = testVolumeHandle.Unlink(RootDirInodeNumber, "symlink_to_link_1_to_file_inode", false)
	if nil != err {
		t.Fatalf("Unlink(RootDirInodeNumber, \"symlink_to_link_1_to_file_inode\", false) failed: %v", err)
	}

	numEntries, err = testVolumeHandle.NumDirEntries(RootDirInodeNumber)

	if nil != err {
		t.Fatalf("NumDirEntries(RootDirInodeNumber) failed: %v", err)
	}
	if 2 != numEntries {
		t.Fatalf("NumDirEntries(RootDirInodeNumber) should have returned numEntries == 2")
	}

	err = testVolumeHandle.Destroy(symlinkInodeToLink1ToFileInode)
	if nil != err {
		t.Fatalf("Destroy(symlinkInodeToLink1ToFileInode) failed: %v", err)
	}

	err = testVolumeHandle.Flush(fileInodeNumber, false)
	if nil != err {
		t.Fatalf("Flush(fileInodeNumber, false) failed: %v", err)
	}
	err = testVolumeHandle.Purge(fileInodeNumber)
	if nil != err {
		t.Fatalf("Purge(fileInodeNumber) failed: %v", err)
	}

	err = testVolumeHandle.PutStream(fileInodeNumber, "test_stream", []byte{0x00, 0x01, 0x02})
	if nil != err {
		t.Fatalf("PutStream(fileInodeNumber, \"test_stream\", []byte{0x00, 0x01, 0x02}) failed: %v", err)
	}

	postMetadata, err = testVolumeHandle.GetMetadata(fileInodeNumber)
	if nil != err {
		t.Fatalf("GetMetadata(fileInodeNumber) failed: %v", err)
	}
	if 1 != len(postMetadata.InodeStreamNameSlice) {
		t.Fatalf("GetMetadata(fileInodeNumber) should have returned postMetadata.InodeStreamNameSlice with 1 element")
	}
	if postMetadata.InodeStreamNameSlice[0] != "test_stream" {
		t.Fatalf("GetMetadata(fileInodeNumber) returned unexpected postMetadata.InodeStreamNameSlice[0]")
	}

	testStreamData, err := testVolumeHandle.GetStream(fileInodeNumber, "test_stream")
	if nil != err {
		t.Fatalf("GetStream(fileInodeNumber, \"test_stream\") failed: %v", err)
	}
	if 0 != bytes.Compare(testStreamData, []byte{0x00, 0x01, 0x02}) {
		t.Fatalf("GetStream(fileInodeNumber, \"test_stream\") returned unexpected testStreamData")
	}

	err = testVolumeHandle.DeleteStream(fileInodeNumber, "test_stream")
	if nil != err {
		t.Fatalf("DeleteStream(fileInodeNumber, \"test_stream\") failed: %v", err)
	}

	postMetadata, err = testVolumeHandle.GetMetadata(fileInodeNumber)
	if nil != err {
		t.Fatalf("GetMetadata(fileInodeNumber) failed: %v", err)
	}
	if 0 != len(postMetadata.InodeStreamNameSlice) {
		t.Fatalf("GetMetadata(fileInodeNumber) should have returned postMetadata.InodeStreamNameSlice with 0 elements")
	}

	testMetadata.CreationTime = time.Now().Add(time.Hour)
	err = testVolumeHandle.SetCreationTime(fileInodeNumber, testMetadata.CreationTime)
	if nil != err {
		t.Fatalf("SetCreationTime(fileInodeNumber, oneHourFromNow) failed: %v", err)
	}
	postMetadata, err = testVolumeHandle.GetMetadata(fileInodeNumber)
	if nil != err {
		t.Fatalf("GetMetadata(fileInodeNumber) failed: %v", err)
	}
	checkMetadata(t, postMetadata, testMetadata, MetadataCrTimeField,
		"GetMetadata() after SetCreationTime() test 1")

	testMetadata.ModificationTime = time.Now().Add(2 * time.Hour)
	err = testVolumeHandle.SetModificationTime(fileInodeNumber, testMetadata.ModificationTime)
	if nil != err {
		t.Fatalf("SetModificationTime(fileInodeNumber, twoHoursFromNow) failed: %v", err)
	}
	postMetadata, err = testVolumeHandle.GetMetadata(fileInodeNumber)
	if nil != err {
		t.Fatalf("GetMetadata(fileInodeNumber) failed: %v", err)
	}
	checkMetadata(t, postMetadata, testMetadata, MetadataModTimeField, "GetMetadata() after SetModificationTime()")

	checkMetadata(t, postMetadata, testMetadata, MetadataNumWritesField, "GetMetadata() before Write()")
	err = testVolumeHandle.Write(fileInodeNumber, 0, []byte{0x00, 0x01, 0x02, 0x03, 0x04}, nil)
	if nil != err {
		t.Fatalf("Write(fileInodeNumber, 0, []byte{0x00, 0x01, 0x02, 0x03, 0x04}) failed: %v", err)
	}
	postMetadata, err = testVolumeHandle.GetMetadata(fileInodeNumber)
	if nil != err {
		t.Fatalf("GetMetadata(fileInodeNumber) failed: %v", err)
	}
	testMetadata.NumWrites = 1
	checkMetadata(t, postMetadata, testMetadata, MetadataNumWritesField, "GetMetadata() after Write()")

	err = testVolumeHandle.Flush(fileInodeNumber, false)
	if err != nil {
		t.Fatalf("Flush(fileInodeNumber) failed: %v", err)
	}

	fileInodeObjectPath, err := testVolumeHandle.ProvisionObject()
	if nil != err {
		t.Fatalf("ProvisionObjectPath() failed: %v", err)
	}

	accountName, containerName, objectName, err := utils.PathToAcctContObj(fileInodeObjectPath)
	if err != nil {
		t.Fatalf("couldn't parse %v as object path", fileInodeObjectPath)
	}

	putContext, err := swiftclient.ObjectFetchChunkedPutContext(accountName, containerName, objectName, "")

	if err != nil {
		t.Fatalf("fetching chunked put context failed")
	}
	err = putContext.SendChunk([]byte{0x11, 0x22, 0x33})
	if err != nil {
		t.Fatalf("sending chunk failed")
	}
	err = putContext.Close()
	if err != nil {
		t.Fatalf("closing chunked PUT failed")
	}

	postMetadata, err = testVolumeHandle.GetMetadata(fileInodeNumber)
	if nil != err {
		t.Fatalf("GetMetadata(fileInodeNumber) failed: %v", err)
	}
	checkMetadata(t, postMetadata, testMetadata, MetadataNumWritesField, "GetMetadata() before Wrote(,,,,true)")
	err = testVolumeHandle.Wrote(fileInodeNumber, fileInodeObjectPath, []uint64{1}, []uint64{0}, []uint64{3}, true)
	if nil != err {
		t.Fatalf("Wrote(fileInodeNumber, fileInodeObjectPath, []uint64{1}, []uint64{0}, []uint64{3}, true) failed: %v", err)
	}
	postMetadata, err = testVolumeHandle.GetMetadata(fileInodeNumber)
	if nil != err {
		t.Fatalf("GetMetadata(fileInodeNumber) failed: %v", err)
	}
	testMetadata.NumWrites = 2
	checkMetadata(t, postMetadata, testMetadata, MetadataNumWritesField, "GetMetadata() after Wrote(,,,,true)")

	testFileInodeData, err := testVolumeHandle.Read(fileInodeNumber, 0, 5, nil)
	if nil != err {
		t.Fatalf("Read(fileInodeNumber, 0, 5) failed: %v", err)
	}
	if 0 != bytes.Compare(testFileInodeData, []byte{0x00, 0x11, 0x22, 0x33, 0x04}) {
		t.Fatalf("Read(fileInodeNumber, 0, 5) returned unexpected testFileInodeData")
	}

	offset := uint64(0)
	length := uint64(5)
	testReadPlan, err := testVolumeHandle.GetReadPlan(fileInodeNumber, &offset, &length)
	if nil != err {
		t.Fatalf("GetReadPlan(fileInodeNumber, 0, 5) failed: %v", err)
	}
	if 3 != len(testReadPlan) {
		t.Fatalf("GetReadPlan(fileInodeNumber, 0, 5) should have returned testReadPlan with 3 elements")
	}
	if (1 != testReadPlan[0].Length) || (fileInodeObjectPath != testReadPlan[1].ObjectPath) || (0 != testReadPlan[1].Offset) || (3 != testReadPlan[1].Length) || (1 != testReadPlan[2].Length) {
		t.Fatalf("GetReadPlan(fileInodeNumber, 0, 5) returned unexpected testReadPlan")
	}

	testExtentMapChunk, err := testVolumeHandle.FetchExtentMapChunk(fileInodeNumber, uint64(2), 2, 1)
	if nil != err {
		t.Fatalf("FetchExtentMap(fileInodeNumber, uint64(2), 2, 1) failed: %v", err)
	}
	if 0 != testExtentMapChunk.FileOffsetRangeStart {
		t.Fatalf("FetchExtentMap(fileInodeNumber, uint64(2), 2, 1) should have returned testExtentMapChunk.FileOffsetRangeStart == 0")
	}
	if 5 != testExtentMapChunk.FileOffsetRangeEnd {
		t.Fatalf("FetchExtentMap(fileInodeNumber, uint64(2), 2, 1) should have returned testExtentMapChunk.FileOffsetRangeEnd == 5")
	}
	if 5 != testExtentMapChunk.FileSize {
		t.Fatalf("FetchExtentMap(fileInodeNumber, uint64(2), 2, 1) should have returned testExtentMapChunk.FileSize == 5")
	}
	if 3 != len(testExtentMapChunk.ExtentMapEntry) {
		t.Fatalf("FetchExtentMap(fileInodeNumber, uint64(2), 2, 1) should have returned testExtentMapChunk.ExtentMapEntry slice with 3 elements")
	}
	fileInodeObjectPathSplit := strings.Split(fileInodeObjectPath, "/")
	fileInodeObjectPathContainerName := fileInodeObjectPathSplit[len(fileInodeObjectPathSplit)-2]
	fileInodeObjectPathObjectName := fileInodeObjectPathSplit[len(fileInodeObjectPathSplit)-1]
	if (0 != testExtentMapChunk.ExtentMapEntry[0].FileOffset) ||
		(0 != testExtentMapChunk.ExtentMapEntry[0].LogSegmentOffset) ||
		(1 != testExtentMapChunk.ExtentMapEntry[0].Length) ||
		(1 != testExtentMapChunk.ExtentMapEntry[1].FileOffset) ||
		(0 != testExtentMapChunk.ExtentMapEntry[1].LogSegmentOffset) ||
		(3 != testExtentMapChunk.ExtentMapEntry[1].Length) ||
		(fileInodeObjectPathContainerName != testExtentMapChunk.ExtentMapEntry[1].ContainerName) ||
		(fileInodeObjectPathObjectName != testExtentMapChunk.ExtentMapEntry[1].ObjectName) ||
		(4 != testExtentMapChunk.ExtentMapEntry[2].FileOffset) ||
		(4 != testExtentMapChunk.ExtentMapEntry[2].LogSegmentOffset) ||
		(1 != testExtentMapChunk.ExtentMapEntry[2].Length) {
		t.Fatalf("FetchExtentMap(fileInodeNumber, uint64(2), 2, 1) returned unexpected testExtentMapChunk.ExtentMapEntry slice")
	}

	// Suffix byte range query, like "Range: bytes=-2"
	length = uint64(3)
	testSuffixReadPlan, err := testVolumeHandle.GetReadPlan(fileInodeNumber, nil, &length)
	if nil != err {
		t.Fatalf("GetReadPlan(fileInodeNumber, nil, 3) failed: %v", err)
	}
	bytesFromReadPlan := []byte{}
	for _, rps := range testSuffixReadPlan {
		pathParts := strings.SplitN(rps.ObjectPath, "/", 5)
		b, err := swiftclient.ObjectGet(pathParts[2], pathParts[3], pathParts[4], rps.Offset, rps.Length)
		if err != nil {
			t.Fatalf("ObjectGet() returned unexpected error %v", err)
		}
		bytesFromReadPlan = append(bytesFromReadPlan, b...)
	}
	if !bytes.Equal(bytesFromReadPlan, []byte{0x22, 0x33, 0x04}) {
		t.Fatalf("expected bytes from suffix read plan 0x22, 0x33, 0x04; got %v", bytesFromReadPlan)
	}

	// Long suffix byte range query, like "Range: bytes=-200" (for a file of size < 200)
	length = uint64(10) // our file has only 5 bytes
	testSuffixReadPlan, err = testVolumeHandle.GetReadPlan(fileInodeNumber, nil, &length)
	if nil != err {
		t.Fatalf("GetReadPlan(fileInodeNumber, nil, 10) failed: %v", err)
	}
	bytesFromReadPlan = []byte{}
	for _, rps := range testSuffixReadPlan {
		pathParts := strings.SplitN(rps.ObjectPath, "/", 5)
		b, err := swiftclient.ObjectGet(pathParts[2], pathParts[3], pathParts[4], rps.Offset, rps.Length)
		if err != nil {
			t.Fatalf("ObjectGet() returned unexpected error %v", err)
		}
		bytesFromReadPlan = append(bytesFromReadPlan, b...)
	}
	if !bytes.Equal(bytesFromReadPlan, []byte{0x00, 0x11, 0x22, 0x33, 0x04}) {
		t.Fatalf("expected bytes from suffix read plan 0x00, 0x11, 0x22, 0x33, 0x04; got %v", bytesFromReadPlan)
	}

	// Prefix byte range query, like "Range: bytes=4-"
	offset = uint64(3)
	testPrefixReadPlan, err := testVolumeHandle.GetReadPlan(fileInodeNumber, &offset, nil)
	if nil != err {
		t.Fatalf("GetReadPlan(fileInodeNumber, 3, nil) failed: %v", err)
	}
	bytesFromReadPlan = []byte{}
	for _, rps := range testPrefixReadPlan {
		pathParts := strings.SplitN(rps.ObjectPath, "/", 5)
		b, err := swiftclient.ObjectGet(pathParts[2], pathParts[3], pathParts[4], rps.Offset, rps.Length)
		if err != nil {
			t.Fatalf("ObjectGet() returned unexpected error %v", err)
		}
		bytesFromReadPlan = append(bytesFromReadPlan, b...)
	}
	if !bytes.Equal(bytesFromReadPlan, []byte{0x33, 0x04}) {
		t.Fatalf("expected bytes from prefix read plan 0x33, 0x04; got %v", bytesFromReadPlan)
	}

	accountName, containerName, objectName, err = utils.PathToAcctContObj(testReadPlan[0].ObjectPath)
	if nil != err {
		t.Fatalf("expected %q to be a legal object path", testReadPlan[0].ObjectPath)
	}

	headSlice, err := swiftclient.ObjectGet(accountName, containerName, objectName, testReadPlan[0].Offset, 1)
	if nil != err {
		t.Fatalf("ObjectGet() returned unexpected error: %v", err)
	}
	if 0 != bytes.Compare(headSlice, []byte{0x00}) {
		t.Fatalf("expected byte of %v to be 0x00, got %x", testReadPlan[0].ObjectPath, headSlice)
	}

	accountName, containerName, objectName, err = utils.PathToAcctContObj(testReadPlan[1].ObjectPath)
	if nil != err {
		t.Fatalf("expected %q to be a legal object path", testReadPlan[1].ObjectPath)
	}
	middleSlice, err := swiftclient.ObjectGet(accountName, containerName, objectName, testReadPlan[1].Offset, 3)
	if nil != err {
		t.Fatalf("ObjectGet() returned unexpected error: %v", err)
	}
	if 0 != bytes.Compare(middleSlice, []byte{0x11, 0x22, 0x33}) {
		t.Fatalf("expected byte of %v to be 0x00, got %x", testReadPlan[0].ObjectPath, headSlice)
	}

	accountName, containerName, objectName, err = utils.PathToAcctContObj(testReadPlan[2].ObjectPath)
	if nil != err {
		t.Fatalf("expected %q to be a legal object path", testReadPlan[2].ObjectPath)
	}
	tailSlice, err := swiftclient.ObjectGet(accountName, containerName, objectName, testReadPlan[2].Offset, 1)
	if nil != err {
		t.Fatalf("objectContext.Get() returned unexpected error: %v", err)
	}
	if 0 != bytes.Compare(tailSlice, []byte{0x04}) {
		t.Fatalf("expected byte of %v to be 0x04, got %x", testReadPlan[0].ObjectPath, tailSlice)
	}

	fileInodeObjectPath, err = testVolumeHandle.ProvisionObject()
	if nil != err {
		t.Fatalf("ProvisionObjectPath() failed: %v", err)
	}

	accountName, containerName, objectName, err = utils.PathToAcctContObj(fileInodeObjectPath)
	if err != nil {
		t.Fatalf("couldn't parse %v as object path", fileInodeObjectPath)
	}

	putContext, err = swiftclient.ObjectFetchChunkedPutContext(accountName, containerName, objectName, "")
	if err != nil {
		t.Fatalf("fetching chunked put context failed")
	}
	err = putContext.SendChunk([]byte{0xFE, 0xFF})
	if err != nil {
		t.Fatalf("sending chunk failed")
	}
	err = putContext.Close()
	if err != nil {
		t.Fatalf("closing chunked PUT failed")
	}

	postMetadata, err = testVolumeHandle.GetMetadata(fileInodeNumber)
	if nil != err {
		t.Fatalf("GetMetadata(fileInodeNumber) failed: %v", err)
	}
	checkMetadata(t, postMetadata, testMetadata, MetadataNumWritesField, "GetMetadata() before Wrote(,,,,false)")
	err = testVolumeHandle.Wrote(fileInodeNumber, fileInodeObjectPath, []uint64{0}, []uint64{0}, []uint64{2}, false)
	if nil != err {
		t.Fatalf("Wrote(fileInodeNumber, fileInodeObjectPath, []uint64{0}, []uint64{0}, []uint64{2}, false) failed: %v", err)
	}
	postMetadata, err = testVolumeHandle.GetMetadata(fileInodeNumber)
	if nil != err {
		t.Fatalf("GetMetadata(fileInodeNumber) failed: %v", err)
	}
	testMetadata.NumWrites = 1
	checkMetadata(t, postMetadata, testMetadata, MetadataNumWritesField, "GetMetadata() after Wrote(,,,,false)")

	testFileInodeData, err = testVolumeHandle.Read(fileInodeNumber, 0, 4, nil)
	if nil != err {
		t.Fatalf("Read(fileInodeNumber, 0, 4) failed: %v", err)
	}
	if 0 != bytes.Compare(testFileInodeData, []byte{0xFE, 0xFF}) {
		t.Fatalf("Read(fileInodeNumber, 0, 4) returned unexpected testFileInodeData")
	}

	preResizeMetadata, err := testVolumeHandle.GetMetadata(fileInodeNumber)
	if nil != err {
		t.Fatalf("GetMetadata(fileInodeNumber) failed: %v", err)
	}

	testMetadata.Size = 0
	err = testVolumeHandle.SetSize(fileInodeNumber, testMetadata.Size)
	if nil != err {
		t.Fatalf("SetSize(fileInodeNumber, 0) failed: %v", err)
	}

	postMetadata, err = testVolumeHandle.GetMetadata(fileInodeNumber)
	if nil != err {
		t.Fatalf("GetMetadata(fileInodeNumber) failed: %v", err)
	}
	checkMetadata(t, postMetadata, testMetadata, MetadataSizeField, "GetMetadata() after SetSize()")
	checkMetadataTimeChanges(t, preResizeMetadata, postMetadata, false, true, false, true, "SetSize changed metadata times inappropriately")

	err = testVolumeHandle.Flush(fileInodeNumber, true)
	if nil != err {
		t.Fatalf("Flush(fileInodeNumber, true) failed: %v", err)
	}

	err = testVolumeHandle.Destroy(fileInodeNumber)
	if nil != err {
		t.Fatalf("Destroy(fileInodeNumber) failed: %v", err)
	}

	file1Inode, err := testVolumeHandle.CreateFile(PosixModePerm, 0, 0)
	if nil != err {
		t.Fatalf("CreateFile() failed: %v", err)
	}

	file2Inode, err := testVolumeHandle.CreateFile(PosixModePerm, 0, 0)
	if nil != err {
		t.Fatalf("CreateFile() failed: %v", err)
	}

	err = testVolumeHandle.Link(RootDirInodeNumber, "1stLocation", file1Inode, false)
	if nil != err {
		t.Fatalf("Link(RootDirInodeNumber, \"1stLocation\", file1Inode, false) failed: %v", err)
	}

	err = testVolumeHandle.Link(RootDirInodeNumber, "3rdLocation", file2Inode, false)
	if nil != err {
		t.Fatalf("Link(RootDirInodeNumber, \"3rdLocation\", file2Inode, false) failed: %v", err)
	}

	dirEntrySlice, moreEntries, err = testVolumeHandle.ReadDir(RootDirInodeNumber, 0, 0)
	if nil != err {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) failed: %v", err)
	}
	if moreEntries {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) should have returned moreEntries == false")
	}
	if 4 != len(dirEntrySlice) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) should have returned dirEntrySlice with 2 elements")
	}
	if (dirEntrySlice[0].InodeNumber != RootDirInodeNumber) || (dirEntrySlice[0].Basename != ".") || (dirEntrySlice[0].NextDirLocation != 1) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) returned unexpected dirEntrySlice[0]")
	}
	if (dirEntrySlice[1].InodeNumber != RootDirInodeNumber) || (dirEntrySlice[1].Basename != "..") || (dirEntrySlice[1].NextDirLocation != 2) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) returned unexpected dirEntrySlice[1]")
	}
	if (dirEntrySlice[2].InodeNumber != file1Inode) || (dirEntrySlice[2].Basename != "1stLocation") || (dirEntrySlice[2].NextDirLocation != 3) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) returned unexpected dirEntrySlice[2]")
	}
	if (dirEntrySlice[3].InodeNumber != file2Inode) || (dirEntrySlice[3].Basename != "3rdLocation") || (dirEntrySlice[3].NextDirLocation != 4) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) returned unexpected dirEntrySlice[2]")
	}

	err = testVolumeHandle.Move(RootDirInodeNumber, "1stLocation", RootDirInodeNumber, "2ndLocation")
	if nil != err {
		t.Fatalf("Move(RootDirInodeNumber, \"1stLocation\", RootDirInodeNumber, \"2ndLocation\") failed: %v", err)
	}

	dirEntrySlice, moreEntries, err = testVolumeHandle.ReadDir(RootDirInodeNumber, 0, 0)
	if nil != err {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) failed: %v", err)
	}
	if moreEntries {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) should have returned moreEntries == false")
	}
	if 4 != len(dirEntrySlice) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) should have returned dirEntrySlice with 2 elements")
	}
	if (dirEntrySlice[0].InodeNumber != RootDirInodeNumber) || (dirEntrySlice[0].Basename != ".") || (dirEntrySlice[0].NextDirLocation != 1) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) returned unexpected dirEntrySlice[0]")
	}
	if (dirEntrySlice[1].InodeNumber != RootDirInodeNumber) || (dirEntrySlice[1].Basename != "..") || (dirEntrySlice[1].NextDirLocation != 2) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) returned unexpected dirEntrySlice[1]")
	}
	if (dirEntrySlice[2].InodeNumber != file1Inode) || (dirEntrySlice[2].Basename != "2ndLocation") || (dirEntrySlice[2].NextDirLocation != 3) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) returned unexpected dirEntrySlice[2]")
	}
	if (dirEntrySlice[3].InodeNumber != file2Inode) || (dirEntrySlice[3].Basename != "3rdLocation") || (dirEntrySlice[3].NextDirLocation != 4) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) returned unexpected dirEntrySlice[2]")
	}

	err = testVolumeHandle.Unlink(RootDirInodeNumber, "3rdLocation", false)
	if nil != err {
		t.Fatalf("Unlink(RootDirInodeNumber, \"3rdLocation\", false) failed: %v", err)
	}
	err = testVolumeHandle.Move(RootDirInodeNumber, "2ndLocation", RootDirInodeNumber, "3rdLocation")
	if nil != err {
		t.Fatalf("Move(RootDirInodeNumber, \"2ndLocation\", RootDirInodeNumber, \"3rdLocation\") failed: %v", err)
	}

	dirEntrySlice, moreEntries, err = testVolumeHandle.ReadDir(RootDirInodeNumber, 0, 0)
	if nil != err {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) failed: %v", err)
	}
	if moreEntries {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) should have returned moreEntries == false")
	}
	if 3 != len(dirEntrySlice) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) should have returned dirEntrySlice with 2 elements")
	}
	if (dirEntrySlice[0].InodeNumber != RootDirInodeNumber) || (dirEntrySlice[0].Basename != ".") || (dirEntrySlice[0].NextDirLocation != 1) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) returned unexpected dirEntrySlice[0]")
	}
	if (dirEntrySlice[1].InodeNumber != RootDirInodeNumber) || (dirEntrySlice[1].Basename != "..") || (dirEntrySlice[1].NextDirLocation != 2) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) returned unexpected dirEntrySlice[1]")
	}
	if (dirEntrySlice[2].InodeNumber != file1Inode) || (dirEntrySlice[2].Basename != "3rdLocation") || (dirEntrySlice[2].NextDirLocation != 3) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) returned unexpected dirEntrySlice[2]")
	}

	err = testVolumeHandle.Destroy(file2Inode)
	if nil != err {
		t.Fatalf("Destroy(file2Inode) failed: %v", err)
	}

	postMetadata, err = testVolumeHandle.GetMetadata(RootDirInodeNumber)
	if nil != err {
		t.Fatalf("GetMetadata(RootDirInodeNumber) failed: %v", err)
	}
	testMetadata.LinkCount = 2
	checkMetadata(t, postMetadata, testMetadata, MetadataLinkCountField, "GetMetadata() after multiple calls to Link()")

	subDirInode, err := testVolumeHandle.CreateDir(PosixModePerm, 0, 0)
	if nil != err {
		t.Fatalf("CreateDir() failed: %v", err)
	}

	subDirInodeType, err := testVolumeHandle.GetType(subDirInode)
	if nil != err {
		t.Fatalf("GetType(subDirInode) failed: %v", err)
	}
	if DirType != subDirInodeType {
		t.Fatalf("GetType(subDirInode) returned unexpected type: %v", subDirInodeType)
	}

	subDirLinkCount, err := testVolumeHandle.GetLinkCount(subDirInode)
	if nil != err {
		t.Fatalf("GetLinkCount(subDirInode) failed: %v", err)
	}
	if 1 != subDirLinkCount {
		t.Fatalf("GetLinkCount(subDirInode) returned unexpected linkCount: %v", subDirLinkCount)
	}

	err = testVolumeHandle.SetLinkCount(subDirInode, uint64(2))
	if nil != err {
		t.Fatalf("SetLinkCount(subDirInode, uint64(2)) failed: %v", err)
	}

	subDirLinkCount, err = testVolumeHandle.GetLinkCount(subDirInode)
	if nil != err {
		t.Fatalf("GetLinkCount(subDirInode) failed: %v", err)
	}
	if 2 != subDirLinkCount {
		t.Fatalf("GetLinkCount(subDirInode) returned unexpected linkCount: %v", subDirLinkCount)
	}

	err = testVolumeHandle.SetLinkCount(subDirInode, uint64(1))
	if nil != err {
		t.Fatalf("SetLinkCount(subDirInode, uint64(1)) failed: %v", err)
	}

	subDirLinkCount, err = testVolumeHandle.GetLinkCount(subDirInode)
	if nil != err {
		t.Fatalf("GetLinkCount(subDirInode) failed: %v", err)
	}
	if 1 != subDirLinkCount {
		t.Fatalf("GetLinkCount(subDirInode) returned unexpected linkCount: %v", subDirLinkCount)
	}

	// it should be illeagal to link to a directory
	err = testVolumeHandle.Link(RootDirInodeNumber, "subDir", subDirInode, false)
	if nil != err {
		t.Fatalf("Link(RootDirInodeNumber, \"subDir\", subDirInode, false) failed: %v", err)
	}

	rootDirLinkCount, err = testVolumeHandle.GetLinkCount(RootDirInodeNumber)
	if nil != err {
		t.Fatalf("GetLinkCount(RootDirInodeNumber) failed: %v", err)
	}
	if 3 != rootDirLinkCount {
		t.Fatalf("GetLinkCount(RootDirInodeNumber) returned unexpected linkCount: %v", rootDirLinkCount)
	}

	subDirLinkCount, err = testVolumeHandle.GetLinkCount(subDirInode)
	if nil != err {
		t.Fatalf("GetLinkCount(subDirInode) failed: %v", err)
	}
	if 2 != subDirLinkCount {
		t.Fatalf("GetLinkCount(subDirInode) returned unexpected linkCount: %v", subDirLinkCount)
	}

	postMetadata, err = testVolumeHandle.GetMetadata(RootDirInodeNumber)
	if nil != err {
		t.Fatalf("GetMetadata(RootDirInodeNumber) failed: %v", err)
	}
	testMetadata.LinkCount = 3
	checkMetadata(t, postMetadata, testMetadata, MetadataLinkCountField, "GetMetadata() after Link()")

	dirEntrySlice, moreEntries, err = testVolumeHandle.ReadDir(RootDirInodeNumber, 0, 0)
	if nil != err {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) failed: %v", err)
	}
	if moreEntries {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) should have returned moreEntries == false")
	}
	if 4 != len(dirEntrySlice) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) should have returned dirEntrySlice with 2 elements, got %#v, which is of length %v", dirEntrySlice, len(dirEntrySlice))
	}
	if (dirEntrySlice[0].InodeNumber != RootDirInodeNumber) || (dirEntrySlice[0].Basename != ".") || (dirEntrySlice[0].NextDirLocation != 1) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) returned unexpected dirEntrySlice[0]")
	}
	if (dirEntrySlice[1].InodeNumber != RootDirInodeNumber) || (dirEntrySlice[1].Basename != "..") || (dirEntrySlice[1].NextDirLocation != 2) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) returned unexpected dirEntrySlice[1]")
	}
	if (dirEntrySlice[2].InodeNumber != file1Inode) || (dirEntrySlice[2].Basename != "3rdLocation") || (dirEntrySlice[2].NextDirLocation != 3) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) returned unexpected dirEntrySlice[2]")
	}
	if (dirEntrySlice[3].InodeNumber != subDirInode) || (dirEntrySlice[3].Basename != "subDir") || (dirEntrySlice[3].NextDirLocation != 4) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) returned unexpected dirEntrySlice[3]")
	}

	dirEntrySlice, moreEntries, err = testVolumeHandle.ReadDir(subDirInode, 0, 0)
	if nil != err {
		t.Fatalf("ReadDir(subDirInode, 0, 0) failed: %v", err)
	}
	if moreEntries {
		t.Fatalf("ReadDir(subDirInode, 0, 0) should have returned moreEntries == false")
	}
	if 2 != len(dirEntrySlice) {
		t.Fatalf("ReadDir(subDirInode, 0, 0) should have returned dirEntrySlice with 2 elements")
	}
	if (dirEntrySlice[0].InodeNumber != subDirInode) || (dirEntrySlice[0].Basename != ".") || (dirEntrySlice[0].NextDirLocation != 1) {
		t.Fatalf("ReadDir(subDirInode, 0, 0) returned unexpected dirEntrySlice[0]")
	}
	if (dirEntrySlice[1].InodeNumber != RootDirInodeNumber) || (dirEntrySlice[1].Basename != "..") || (dirEntrySlice[1].NextDirLocation != 2) {
		t.Fatalf("ReadDir(subDirInode, 0, 0) returned unexpected dirEntrySlice[1]")
	}

	err = testVolumeHandle.Move(RootDirInodeNumber, "3rdLocation", subDirInode, "4thLocation")
	if nil != err {
		t.Fatalf("Move(RootDirInodeNumber, \"3rdLocation\", subDirInode, \"4thLocation\") failed: %v", err)
	}

	dirEntrySlice, moreEntries, err = testVolumeHandle.ReadDir(RootDirInodeNumber, 0, 0)
	if nil != err {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) failed: %v", err)
	}
	if moreEntries {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) should have returned moreEntries == false")
	}
	if 3 != len(dirEntrySlice) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) should have returned dirEntrySlice with 2 elements, got %#v", dirEntrySlice)
	}
	if (dirEntrySlice[0].InodeNumber != RootDirInodeNumber) || (dirEntrySlice[0].Basename != ".") || (dirEntrySlice[0].NextDirLocation != 1) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) returned unexpected dirEntrySlice[0]")
	}
	if (dirEntrySlice[1].InodeNumber != RootDirInodeNumber) || (dirEntrySlice[1].Basename != "..") || (dirEntrySlice[1].NextDirLocation != 2) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) returned unexpected dirEntrySlice[1]")
	}
	if (dirEntrySlice[2].InodeNumber != subDirInode) || (dirEntrySlice[2].Basename != "subDir") || (dirEntrySlice[2].NextDirLocation != 3) {
		t.Fatalf("ReadDir(RootDirInodeNumber, 0, 0) returned unexpected dirEntrySlice[2]")
	}

	dirEntrySlice, moreEntries, err = testVolumeHandle.ReadDir(subDirInode, 0, 0)
	if nil != err {
		t.Fatalf("ReadDir(subDirInode, 0, 0) failed: %v", err)
	}
	if moreEntries {
		t.Fatalf("ReadDir(subDirInode, 0, 0) should have returned moreEntries == false")
	}
	if 3 != len(dirEntrySlice) {
		t.Fatalf("ReadDir(subDirInode, 0, 0) should have returned dirEntrySlice with 2 elements")
	}
	if (dirEntrySlice[0].InodeNumber != subDirInode) || (dirEntrySlice[0].Basename != ".") || (dirEntrySlice[0].NextDirLocation != 1) {
		t.Fatalf("ReadDir(subDirInode, 0, 0) returned unexpected dirEntrySlice[0]")
	}
	if (dirEntrySlice[1].InodeNumber != RootDirInodeNumber) || (dirEntrySlice[1].Basename != "..") || (dirEntrySlice[1].NextDirLocation != 2) {
		t.Fatalf("ReadDir(subDirInode, 0, 0) returned unexpected dirEntrySlice[1]")
	}
	if (dirEntrySlice[2].InodeNumber != file1Inode) || (dirEntrySlice[2].Basename != "4thLocation") || (dirEntrySlice[2].NextDirLocation != 3) {
		t.Fatalf("ReadDir(subDirInode, 0, 0) returned unexpected dirEntrySlice[2]")
	}

	err = testVolumeHandle.Unlink(subDirInode, "4thLocation", false)
	if nil != err {
		t.Fatalf("Unlink(subDirInode, \"4thLocation\", false) failed: %v", err)
	}

	err = testVolumeHandle.Destroy(file1Inode)
	if nil != err {
		t.Fatalf("Destroy(file1Inode) failed: %v", err)
	}

	err = testVolumeHandle.Unlink(RootDirInodeNumber, "subDir", false)
	if nil != err {
		t.Fatalf("Unlink(RootDirInodeNumber, \"subDir\", false) failed: %v", err)
	}

	rootDirLinkCount, err = testVolumeHandle.GetLinkCount(RootDirInodeNumber)
	if nil != err {
		t.Fatalf("GetLinkCount(RootDirInodeNumber) failed: %v", err)
	}
	if 2 != rootDirLinkCount {
		t.Fatalf("GetLinkCount(RootDirInodeNumber) returned unexpected linkCount: %v", rootDirLinkCount)
	}

	subDirLinkCount, err = testVolumeHandle.GetLinkCount(subDirInode)
	if nil != err {
		t.Fatalf("GetLinkCount(subDirInode) failed: %v", err)
	}
	if 1 != subDirLinkCount {
		t.Fatalf("GetLinkCount(subDirInode) returned unexpected linkCount: %v", subDirLinkCount)
	}

	err = testVolumeHandle.Destroy(subDirInode)
	if nil != err {
		t.Fatalf("Destroy(subDirInode) failed: %v", err)
	}

	positiveDurationToDelayOrSkew, err := time.ParseDuration("+" + durationToDelayOrSkew)
	if nil != err {
		t.Fatalf("time.ParseDuration(\"100ms\") failed: %v", err)
	}
	negativeDurationToDelayOrSkew, err := time.ParseDuration("-" + durationToDelayOrSkew)
	if nil != err {
		t.Fatalf("time.ParseDuration(\"-100ms\") failed: %v", err)
	}

	timeBeforeCreateDir := time.Now()

	time.Sleep(positiveDurationToDelayOrSkew)

	testMetadata.Mode = InodeMode(0744)
	testMetadata.UserID = InodeUserID(987)
	testMetadata.GroupID = InodeGroupID(654)
	dirInode, err := testVolumeHandle.CreateDir(testMetadata.Mode, testMetadata.UserID, testMetadata.GroupID)
	if nil != err {
		t.Fatalf("CreateDir() failed: %v", err)
	}

	time.Sleep(positiveDurationToDelayOrSkew)

	timeAfterCreateDir := time.Now()

	dirInodeMetadataAfterCreateDir, err := testVolumeHandle.GetMetadata(dirInode)
	if nil != err {
		t.Fatalf("GetMetadata(dirInode) failed: %v", err)
	}
	if dirInodeMetadataAfterCreateDir.CreationTime.Before(timeBeforeCreateDir) || dirInodeMetadataAfterCreateDir.CreationTime.After(timeAfterCreateDir) {
		t.Fatalf("dirInodeMetadataAfterCreateDir.CreationTime unexpected")
	}
	if !dirInodeMetadataAfterCreateDir.ModificationTime.Equal(dirInodeMetadataAfterCreateDir.CreationTime) {
		t.Fatalf("dirInodeMetadataAfterCreateDir.ModificationTime unexpected")
	}
	if !dirInodeMetadataAfterCreateDir.AccessTime.Equal(dirInodeMetadataAfterCreateDir.CreationTime) {
		t.Fatalf("dirInodeMetadataAfterCreateDir.AccessTime unexpected")
	}
	if !dirInodeMetadataAfterCreateDir.AttrChangeTime.Equal(dirInodeMetadataAfterCreateDir.CreationTime) {
		t.Fatalf("dirInodeMetadataAfterCreateDir.AttrChangeTime unexpected")
	}
	checkMetadata(t, dirInodeMetadataAfterCreateDir, testMetadata, MetadataPermFields, "GetMetadata() after CreateDir()")

	// XXX TODO: Add more tests related to mode/uid/gid and CreateDir(): mode > 0777, etc.

	testMetadata.CreationTime = dirInodeMetadataAfterCreateDir.CreationTime.Add(negativeDurationToDelayOrSkew)
	testMetadata.ModificationTime = dirInodeMetadataAfterCreateDir.ModificationTime
	testMetadata.AccessTime = dirInodeMetadataAfterCreateDir.AccessTime
	testMetadata.AttrChangeTime = dirInodeMetadataAfterCreateDir.AttrChangeTime

	timeBeforeOp = time.Now()
	time.Sleep(positiveDurationToDelayOrSkew)

	err = testVolumeHandle.SetCreationTime(dirInode, testMetadata.CreationTime)
	if nil != err {
		t.Fatalf("SetCreationTime(dirInode,) failed: %v", err)
	}
	time.Sleep(positiveDurationToDelayOrSkew)
	timeAfterOp = time.Now()

	dirInodeMetadataAfterSetCreationTime, err := testVolumeHandle.GetMetadata(dirInode)
	if nil != err {
		t.Fatalf("GetMetadata(dirInode) failed: %v", err)
	}
	checkMetadata(t, dirInodeMetadataAfterSetCreationTime, testMetadata, MetadataNotAttrTimeFields,
		"GetMetadata() after SetCreationTime() test 2")
	if dirInodeMetadataAfterSetCreationTime.AttrChangeTime.Before(timeBeforeOp) ||
		dirInodeMetadataAfterSetCreationTime.AttrChangeTime.After(timeAfterOp) {
		t.Fatalf("dirInodeMetadataAfterSetCreationTime.AttrChangeTime unexpected")
	}

	testMetadata.ModificationTime = dirInodeMetadataAfterSetCreationTime.ModificationTime.Add(negativeDurationToDelayOrSkew)
	timeBeforeOp = time.Now()
	time.Sleep(positiveDurationToDelayOrSkew)

	err = testVolumeHandle.SetModificationTime(dirInode, testMetadata.ModificationTime)
	if nil != err {
		t.Fatalf("SetModificationTime(dirInode,) failed: %v", err)
	}
	time.Sleep(positiveDurationToDelayOrSkew)
	timeAfterOp = time.Now()

	dirInodeMetadataAfterSetModificationTime, err := testVolumeHandle.GetMetadata(dirInode)
	if nil != err {
		t.Fatalf("GetMetadata(dirInode) failed: %v", err)
	}
	checkMetadata(t, dirInodeMetadataAfterSetModificationTime, testMetadata, MetadataNotAttrTimeFields, "GetMetadata() after SetModificationTime()")
	if dirInodeMetadataAfterSetModificationTime.AttrChangeTime.Before(timeBeforeOp) ||
		dirInodeMetadataAfterSetModificationTime.AttrChangeTime.After(timeAfterOp) {
		t.Fatalf("dirInodeMetadataAfterSetModificationTime.AttrChangeTime unexpected")
	}

	testMetadata.AccessTime = dirInodeMetadataAfterSetModificationTime.AccessTime.Add(negativeDurationToDelayOrSkew)
	timeBeforeOp = time.Now()
	time.Sleep(positiveDurationToDelayOrSkew)

	err = testVolumeHandle.SetAccessTime(dirInode, testMetadata.AccessTime)
	if nil != err {
		t.Fatalf("SetAccessTime(dirInode,) failed: %v", err)
	}
	time.Sleep(positiveDurationToDelayOrSkew)
	timeAfterOp = time.Now()

	dirInodeMetadataAfterSetAccessTime, err := testVolumeHandle.GetMetadata(dirInode)
	if nil != err {
		t.Fatalf("GetMetadata(dirInode) failed: %v", err)
	}
	checkMetadata(t, dirInodeMetadataAfterSetAccessTime, testMetadata, MetadataNotAttrTimeFields, "GetMetadata() after testVolumeHandle.SetAccessTime()")
	if dirInodeMetadataAfterSetAccessTime.AttrChangeTime.Before(timeBeforeOp) ||
		dirInodeMetadataAfterSetAccessTime.AttrChangeTime.After(timeAfterOp) {
		t.Fatalf("dirInodeMetadataAfterSetAccessTime.AttrChangeTime unexpected")
	}

	timeBeforeDirInodePutStream := time.Now()

	time.Sleep(positiveDurationToDelayOrSkew)

	err = testVolumeHandle.PutStream(dirInode, "stream_name", []byte{})
	if nil != err {
		t.Fatalf("PutStream(dirInode,,) failed: %v", err)
	}
	time.Sleep(positiveDurationToDelayOrSkew)

	timeAfterDirInodePutStream := time.Now()

	dirInodeMetadataAfterPutStream, err := testVolumeHandle.GetMetadata(dirInode)
	if nil != err {
		t.Fatalf("GetMetadata(dirInode) failed: %v", err)
	}
	testMetadata.CreationTime = dirInodeMetadataAfterSetAccessTime.CreationTime
	testMetadata.ModificationTime = dirInodeMetadataAfterSetAccessTime.ModificationTime
	testMetadata.AccessTime = dirInodeMetadataAfterSetAccessTime.AccessTime
	checkMetadata(t, dirInodeMetadataAfterPutStream, testMetadata, MetadataNotAttrTimeFields, "GetMetadata() after PutStream()")
	if dirInodeMetadataAfterPutStream.AttrChangeTime.Before(timeBeforeDirInodePutStream) || dirInodeMetadataAfterPutStream.AttrChangeTime.After(timeAfterDirInodePutStream) {
		t.Fatalf("dirInodeMetadataAfterPutStream.AttrChangeTime unexpected")
	}

	timeBeforeDirInodeDeleteStream := time.Now()

	time.Sleep(positiveDurationToDelayOrSkew)

	err = testVolumeHandle.DeleteStream(dirInode, "stream_name")
	if nil != err {
		t.Fatalf("DeleteStream(dirInode,) failed: %v", err)
	}

	time.Sleep(positiveDurationToDelayOrSkew)

	timeAfterDirInodeDeleteStream := time.Now()

	dirInodeMetadataAfterDeleteStream, err := testVolumeHandle.GetMetadata(dirInode)
	if nil != err {
		t.Fatalf("GetMetadata(dirInode) failed: %v", err)
	}
	checkMetadata(t, dirInodeMetadataAfterPutStream, testMetadata, MetadataNotAttrTimeFields, "GetMetadata() after PutStream()")
	if dirInodeMetadataAfterDeleteStream.AttrChangeTime.Before(timeBeforeDirInodeDeleteStream) || dirInodeMetadataAfterDeleteStream.AttrChangeTime.After(timeAfterDirInodeDeleteStream) {
		t.Fatalf("dirInodeMetadataAfterDeleteStream.AttrChangeTime unexpected")
	}

	timeBeforeCreateFile := time.Now()

	time.Sleep(positiveDurationToDelayOrSkew)

	fileInode, err := testVolumeHandle.CreateFile(PosixModePerm, 0, 0)
	if nil != err {
		t.Fatalf("CreateFile() failed: %v", err)
	}

	time.Sleep(positiveDurationToDelayOrSkew)

	timeAfterCreateFile := time.Now()

	fileInodeMetadataAfterCreateFile, err := testVolumeHandle.GetMetadata(fileInode)
	if nil != err {
		t.Fatalf("GetMetadata(fileInode) failed: %v", err)
	}
	if fileInodeMetadataAfterCreateFile.CreationTime.Before(timeBeforeCreateFile) || fileInodeMetadataAfterCreateFile.CreationTime.After(timeAfterCreateFile) {
		t.Fatalf("fileInodeMetadataAfterCreateDir.CreationTime unexpected")
	}
	if !fileInodeMetadataAfterCreateFile.ModificationTime.Equal(fileInodeMetadataAfterCreateFile.CreationTime) {
		t.Fatalf("fileInodeMetadataAfterCreateDir.ModificationTime unexpected")
	}
	if !fileInodeMetadataAfterCreateFile.AccessTime.Equal(fileInodeMetadataAfterCreateFile.CreationTime) {
		t.Fatalf("fileInodeMetadataAfterCreateDir.AccessTime unexpected")
	}
	if !fileInodeMetadataAfterCreateFile.AttrChangeTime.Equal(fileInodeMetadataAfterCreateFile.CreationTime) {
		t.Fatalf("fileInodeMetadataAfterCreateDir.AttrChangeTime unexpected")
	}

	testMetadata.CreationTime = fileInodeMetadataAfterCreateFile.CreationTime.Add(negativeDurationToDelayOrSkew)
	testMetadata.ModificationTime = fileInodeMetadataAfterCreateFile.ModificationTime
	testMetadata.AccessTime = fileInodeMetadataAfterCreateFile.AccessTime
	testMetadata.AttrChangeTime = fileInodeMetadataAfterCreateFile.AttrChangeTime

	timeBeforeOp = time.Now()
	time.Sleep(positiveDurationToDelayOrSkew)

	err = testVolumeHandle.SetCreationTime(fileInode, testMetadata.CreationTime)
	if nil != err {
		t.Fatalf("SetCreationTime(fileInode,) failed: %v", err)
	}
	time.Sleep(positiveDurationToDelayOrSkew)
	timeAfterOp = time.Now()

	fileInodeMetadataAfterSetCreationTime, err := testVolumeHandle.GetMetadata(fileInode)
	if nil != err {
		t.Fatalf("GetMetadata(fileInode) failed: %v", err)
	}
	checkMetadata(t, fileInodeMetadataAfterSetCreationTime, testMetadata, MetadataNotAttrTimeFields,
		"GetMetadata() after SetCreationTime() test 3")
	if fileInodeMetadataAfterSetCreationTime.AttrChangeTime.Before(timeBeforeOp) ||
		fileInodeMetadataAfterSetCreationTime.AttrChangeTime.After(timeAfterOp) {
		t.Fatalf("fileInodeMetadataAfterSetCreationTime.AttrChangeTime unexpected")
	}

	testMetadata.ModificationTime = fileInodeMetadataAfterSetCreationTime.ModificationTime.Add(negativeDurationToDelayOrSkew)
	err = testVolumeHandle.SetModificationTime(fileInode, testMetadata.ModificationTime)
	if nil != err {
		t.Fatalf("SetModificationTime(fileInode,) failed: %v", err)
	}
	fileInodeMetadataAfterSetModificationTime, err := testVolumeHandle.GetMetadata(fileInode)
	if nil != err {
		t.Fatalf("GetMetadata(fileInode) failed: %v", err)
	}
	checkMetadata(t, fileInodeMetadataAfterSetModificationTime, testMetadata, MetadataNotAttrTimeFields, "GetMetadata() after SetModificationTime()")

	testMetadata.AccessTime = fileInodeMetadataAfterSetModificationTime.AccessTime.Add(negativeDurationToDelayOrSkew)
	err = testVolumeHandle.SetAccessTime(fileInode, testMetadata.AccessTime)
	if nil != err {
		t.Fatalf("SetAccessTime(fileInode,) failed: %v", err)
	}
	fileInodeMetadataAfterSetAccessTime, err := testVolumeHandle.GetMetadata(fileInode)
	if nil != err {
		t.Fatalf("GetMetadata(fileInode) failed: %v", err)
	}
	checkMetadata(t, fileInodeMetadataAfterSetAccessTime, testMetadata, MetadataNotAttrTimeFields, "GetMetadata() after SetAccessTime() test 1")

	timeBeforeFileInodePutStream := time.Now()

	time.Sleep(positiveDurationToDelayOrSkew)

	err = testVolumeHandle.PutStream(fileInode, "stream_name", []byte{})
	if nil != err {
		t.Fatalf("PutStream(fileInode,,) failed: %v", err)
	}

	time.Sleep(positiveDurationToDelayOrSkew)

	timeAfterFileInodePutStream := time.Now()

	fileInodeMetadataAfterPutStream, err := testVolumeHandle.GetMetadata(fileInode)
	if nil != err {
		t.Fatalf("GetMetadata(fileInode) failed: %v", err)
	}
	checkMetadata(t, fileInodeMetadataAfterPutStream, testMetadata, MetadataNotAttrTimeFields, "GetMetadata() after PutStream()")
	if fileInodeMetadataAfterPutStream.AttrChangeTime.Before(timeBeforeFileInodePutStream) || fileInodeMetadataAfterPutStream.AttrChangeTime.After(timeAfterFileInodePutStream) {
		t.Fatalf("fileInodeMetadataAfterPutStream.AttrChangeTime unexpected")
	}

	timeBeforeFileInodeDeleteStream := time.Now()

	time.Sleep(positiveDurationToDelayOrSkew)

	err = testVolumeHandle.DeleteStream(fileInode, "stream_name")
	if nil != err {
		t.Fatalf("DeleteStream(fileInode,) failed: %v", err)
	}

	time.Sleep(positiveDurationToDelayOrSkew)

	timeAfterFileInodeDeleteStream := time.Now()

	fileInodeMetadataAfterDeleteStream, err := testVolumeHandle.GetMetadata(fileInode)
	if nil != err {
		t.Fatalf("GetMetadata(fileInode) failed: %v", err)
	}
	checkMetadata(t, fileInodeMetadataAfterDeleteStream, testMetadata, MetadataNotAttrTimeFields, "GetMetadata() after DeleteStream()")
	if fileInodeMetadataAfterDeleteStream.AttrChangeTime.Before(timeBeforeFileInodeDeleteStream) || fileInodeMetadataAfterDeleteStream.AttrChangeTime.After(timeAfterFileInodeDeleteStream) {
		t.Fatalf("fileInodeMetadataAfterDeleteStream.AttrChangeTime unexpected")
	}

	timeBeforeCreateSymlink := time.Now()

	time.Sleep(positiveDurationToDelayOrSkew)

	testMetadata.Mode = PosixModePerm
	testMetadata.UserID = InodeUserID(1111)
	testMetadata.GroupID = InodeGroupID(7777)
	symlinkInode, err := testVolumeHandle.CreateSymlink("nowhere", testMetadata.Mode, testMetadata.UserID, testMetadata.GroupID)
	if nil != err {
		t.Fatalf("CreateSymlink(\"nowhere\") failed: %v", err)
	}

	time.Sleep(positiveDurationToDelayOrSkew)

	timeAfterCreateSymlink := time.Now()

	symlinkInodeMetadataAfterCreateSymlink, err := testVolumeHandle.GetMetadata(symlinkInode)
	if nil != err {
		t.Fatalf("GetMetadata(symlinkInode) failed: %v", err)
	}
	if symlinkInodeMetadataAfterCreateSymlink.CreationTime.Before(timeBeforeCreateSymlink) || symlinkInodeMetadataAfterCreateSymlink.CreationTime.After(timeAfterCreateSymlink) {
		t.Fatalf("symlinkInodeMetadataAfterCreateDir.CreationTime unexpected")
	}
	if !symlinkInodeMetadataAfterCreateSymlink.ModificationTime.Equal(symlinkInodeMetadataAfterCreateSymlink.CreationTime) {
		t.Fatalf("symlinkInodeMetadataAfterCreateDir.ModificationTime unexpected")
	}
	if !symlinkInodeMetadataAfterCreateSymlink.AccessTime.Equal(symlinkInodeMetadataAfterCreateSymlink.CreationTime) {
		t.Fatalf("symlinkInodeMetadataAfterCreateDir.AccessTime unexpected")
	}
	if !symlinkInodeMetadataAfterCreateSymlink.AttrChangeTime.Equal(symlinkInodeMetadataAfterCreateSymlink.CreationTime) {
		t.Fatalf("symlinkInodeMetadataAfterCreateDir.AttrChangeTime unexpected")
	}
	checkMetadata(t, symlinkInodeMetadataAfterCreateSymlink, testMetadata, MetadataPermFields, "GetMetadata() after CreateSymlink()")

	// XXX TODO: Add more tests related to mode/uid/gid and CreateFile(): mode > 0777, etc.

	testMetadata.CreationTime = symlinkInodeMetadataAfterCreateSymlink.CreationTime.Add(negativeDurationToDelayOrSkew)
	testMetadata.ModificationTime = symlinkInodeMetadataAfterCreateSymlink.ModificationTime
	testMetadata.AccessTime = symlinkInodeMetadataAfterCreateSymlink.AccessTime
	testMetadata.AttrChangeTime = symlinkInodeMetadataAfterCreateSymlink.AttrChangeTime
	timeBeforeOp = time.Now()
	time.Sleep(positiveDurationToDelayOrSkew)

	err = testVolumeHandle.SetCreationTime(symlinkInode, testMetadata.CreationTime)
	if nil != err {
		t.Fatalf("SetCreationTime(symlinkInode,) failed: %v", err)
	}
	time.Sleep(positiveDurationToDelayOrSkew)
	timeAfterOp = time.Now()

	symlinkInodeMetadataAfterSetCreationTime, err := testVolumeHandle.GetMetadata(symlinkInode)
	if nil != err {
		t.Fatalf("GetMetadata(symlinkInode) failed: %v", err)
	}
	checkMetadata(t, symlinkInodeMetadataAfterSetCreationTime, testMetadata, MetadataNotAttrTimeFields,
		"GetMetadata() after SetCreationTime() test 4")
	if symlinkInodeMetadataAfterSetCreationTime.AttrChangeTime.Before(timeBeforeOp) ||
		symlinkInodeMetadataAfterSetCreationTime.AttrChangeTime.After(timeAfterOp) {
		t.Fatalf("symlinkInodeMetadataAfterSetCreationTime.AttrChangeTime unexpected")
	}

	testMetadata.ModificationTime = symlinkInodeMetadataAfterSetCreationTime.ModificationTime.Add(negativeDurationToDelayOrSkew)
	err = testVolumeHandle.SetModificationTime(symlinkInode, testMetadata.ModificationTime)
	if nil != err {
		t.Fatalf("SetModificationTime(symlinkInode,) failed: %v", err)
	}
	symlinkInodeMetadataAfterSetModificationTime, err := testVolumeHandle.GetMetadata(symlinkInode)
	if nil != err {
		t.Fatalf("GetMetadata(symlinkInode) failed: %v", err)
	}
	checkMetadata(t, symlinkInodeMetadataAfterSetModificationTime, testMetadata, MetadataNotAttrTimeFields, "GetMetadata() after SetModificationTime()")

	testMetadata.AccessTime = symlinkInodeMetadataAfterSetModificationTime.AccessTime.Add(negativeDurationToDelayOrSkew)
	err = testVolumeHandle.SetAccessTime(symlinkInode, testMetadata.AccessTime)
	if nil != err {
		t.Fatalf("SetAccessTime(symlinkInode,) failed: %v", err)
	}
	symlinkInodeMetadataAfterSetAccessTime, err := testVolumeHandle.GetMetadata(symlinkInode)
	if nil != err {
		t.Fatalf("GetMetadata(symlinkInode) failed: %v", err)
	}
	checkMetadata(t, symlinkInodeMetadataAfterSetAccessTime, testMetadata, MetadataNotAttrTimeFields, "GetMetadata() after SetAccessTime() test 2")

	timeBeforeSymlinkInodePutStream := time.Now()

	time.Sleep(positiveDurationToDelayOrSkew)

	err = testVolumeHandle.PutStream(symlinkInode, "stream_name", []byte{})
	if nil != err {
		t.Fatalf("PutStream(symlinkInode,,) failed: %v", err)
	}

	time.Sleep(positiveDurationToDelayOrSkew)

	timeAfterSymlinkInodePutStream := time.Now()

	symlinkInodeMetadataAfterPutStream, err := testVolumeHandle.GetMetadata(symlinkInode)
	if nil != err {
		t.Fatalf("GetMetadata(symlinkInode) failed: %v", err)
	}
	checkMetadata(t, symlinkInodeMetadataAfterPutStream, testMetadata, MetadataNotAttrTimeFields, "GetMetadata() after PutStream()")
	if symlinkInodeMetadataAfterPutStream.AttrChangeTime.Before(timeBeforeSymlinkInodePutStream) || symlinkInodeMetadataAfterPutStream.AttrChangeTime.After(timeAfterSymlinkInodePutStream) {
		t.Fatalf("symlinkInodeMetadataAfterPutStream.AttrChangeTime unexpected")
	}

	timeBeforeSymlinkInodeDeleteStream := time.Now()

	time.Sleep(positiveDurationToDelayOrSkew)

	err = testVolumeHandle.DeleteStream(symlinkInode, "stream_name")
	if nil != err {
		t.Fatalf("DeleteStream(symlinkInode,) failed: %v", err)
	}

	time.Sleep(positiveDurationToDelayOrSkew)

	timeAfterSymlinkInodeDeleteStream := time.Now()

	symlinkInodeMetadataAfterDeleteStream, err := testVolumeHandle.GetMetadata(symlinkInode)
	if nil != err {
		t.Fatalf("GetMetadata(symlinkInode) failed: %v", err)
	}
	checkMetadata(t, symlinkInodeMetadataAfterDeleteStream, testMetadata, MetadataNotAttrTimeFields, "GetMetadata() after DeleteStream()")
	if symlinkInodeMetadataAfterDeleteStream.AttrChangeTime.Before(timeBeforeSymlinkInodeDeleteStream) || symlinkInodeMetadataAfterDeleteStream.AttrChangeTime.After(timeAfterSymlinkInodeDeleteStream) {
		t.Fatalf("symlinkInodeMetadataAfterDeleteStream.AttrChangeTime unexpected")
	}

	timeBeforeLink := time.Now()

	time.Sleep(positiveDurationToDelayOrSkew)

	err = testVolumeHandle.Link(dirInode, "loc_1", fileInode, false)
	if nil != err {
		t.Fatalf("Link(dirInode, \"loc_1\", fileInode, false) failed: %v", err)
	}

	time.Sleep(positiveDurationToDelayOrSkew)

	timeAfterLink := time.Now()

	dirInodeMetadataAfterLink, err := testVolumeHandle.GetMetadata(dirInode)
	if nil != err {
		t.Fatalf("GetMetadata(dirInode) failed: %v", err)
	}
	fileInodeMetadataAfterLink, err := testVolumeHandle.GetMetadata(fileInode)
	if nil != err {
		t.Fatalf("GetMetadata(fileInode) failed: %v", err)
	}

	if !dirInodeMetadataAfterLink.CreationTime.Equal(dirInodeMetadataAfterDeleteStream.CreationTime) {
		t.Fatalf("dirInodeMetadataAfterLink.CreationTime unexpected")
	}
	if dirInodeMetadataAfterLink.ModificationTime.Before(timeBeforeLink) || dirInodeMetadataAfterLink.ModificationTime.After(timeAfterLink) {
		t.Fatalf("dirInodeMetadataAfterLink.ModificationTime unexpected")
	}
	if !dirInodeMetadataAfterLink.AccessTime.Equal(dirInodeMetadataAfterDeleteStream.AccessTime) {
		t.Fatalf("dirInodeMetadataAfterLink.AccessTime unexpected changed")
	}
	if dirInodeMetadataAfterLink.AttrChangeTime.Before(timeBeforeLink) || dirInodeMetadataAfterLink.AttrChangeTime.After(timeAfterLink) {
		t.Fatalf("dirInodeMetadataAfterLink.AttrChangeTime unexpected")
	}
	if !dirInodeMetadataAfterLink.AttrChangeTime.Equal(dirInodeMetadataAfterLink.ModificationTime) {
		t.Fatalf("dirInodeMetadataAfterLink.AttrChangeTime should equal dirInodeMetadataAfterLink.ModificationTime")
	}
	if !fileInodeMetadataAfterLink.CreationTime.Equal(fileInodeMetadataAfterDeleteStream.CreationTime) {
		t.Fatalf("fileInodeMetadataAfterLink.CreationTime unexpected")
	}
	if !fileInodeMetadataAfterLink.ModificationTime.Equal(fileInodeMetadataAfterDeleteStream.ModificationTime) {
		t.Fatalf("fileInodeMetadataAfterLink.ModificationTime unexpected")
	}
	if !fileInodeMetadataAfterLink.AccessTime.Equal(fileInodeMetadataAfterDeleteStream.AccessTime) {
		t.Fatalf("fileInodeMetadataAfterLink.AccessTime unexpected")
	}
	if fileInodeMetadataAfterLink.AttrChangeTime.Before(timeBeforeLink) || fileInodeMetadataAfterLink.AttrChangeTime.After(timeAfterLink) {
		t.Fatalf("fileInodeMetadataAfterLink.AttrChangeTime unexpected")
	}

	time.Sleep(positiveDurationToDelayOrSkew)

	err = testVolumeHandle.Move(dirInode, "loc_1", dirInode, "loc_2")
	if nil != err {
		t.Fatalf("Move(dirInode, \"loc_1\", dirInode, \"loc_2\") failed: %v", err)
	}

	time.Sleep(positiveDurationToDelayOrSkew)

	timeAfterMove := time.Now()

	dirInodeMetadataAfterMove, err := testVolumeHandle.GetMetadata(dirInode)
	if nil != err {
		t.Fatalf("GetMetadata(dirInode) failed: %v", err)
	}
	fileInodeMetadataAfterMove, err := testVolumeHandle.GetMetadata(fileInode)
	if nil != err {
		t.Fatalf("GetMetadata(fileInode) failed: %v", err)
	}

	if !dirInodeMetadataAfterMove.CreationTime.Equal(dirInodeMetadataAfterLink.CreationTime) {
		t.Fatalf("dirInodeMetadataAfterMove.CreationTime unexpected")
	}
	if dirInodeMetadataAfterMove.ModificationTime.Before(timeAfterLink) || dirInodeMetadataAfterMove.ModificationTime.After(timeAfterMove) {
		t.Fatalf("dirInodeMetadataAfterMove.ModificationTime unexpected")
	}
	if !dirInodeMetadataAfterMove.AccessTime.Equal(dirInodeMetadataAfterLink.AccessTime) {
		t.Fatalf("dirInodeMetadataAfterMove.AccessTime unexpected change")
	}
	if dirInodeMetadataAfterMove.AttrChangeTime.Equal(dirInodeMetadataAfterLink.AttrChangeTime) {
		t.Fatalf("dirInodeMetadataAfterMove.AttrChangeTime unchanged")
	}
	if !fileInodeMetadataAfterMove.CreationTime.Equal(fileInodeMetadataAfterLink.CreationTime) {
		t.Fatalf("fileInodeMetadataAfterMove.CreationTime unexpected")
	}
	if !fileInodeMetadataAfterMove.ModificationTime.Equal(fileInodeMetadataAfterLink.ModificationTime) {
		t.Fatalf("fileInodeMetadataAfterMove.ModificationTime unexpected")
	}
	if !fileInodeMetadataAfterMove.AccessTime.Equal(fileInodeMetadataAfterLink.AccessTime) {
		t.Fatalf("fileInodeMetadataAfterMove.AccessTime unexpected")
	}
	if fileInodeMetadataAfterMove.AttrChangeTime.Equal(fileInodeMetadataAfterLink.AttrChangeTime) {
		t.Fatalf("fileInodeMetadataAfterMove.AttrChangeTime should change after move")
	}

	time.Sleep(positiveDurationToDelayOrSkew)

	err = testVolumeHandle.Unlink(dirInode, "loc_2", false)
	if nil != err {
		t.Fatalf("Unlink(dirInode, \"loc_2\", false) failed: %v", err)
	}

	time.Sleep(positiveDurationToDelayOrSkew)

	timeAfterUnlink := time.Now()

	dirInodeMetadataAfterUnlink, err := testVolumeHandle.GetMetadata(dirInode)
	if nil != err {
		t.Fatalf("GetMetadata(dirInode) failed: %v", err)
	}
	fileInodeMetadataAfterUnlink, err := testVolumeHandle.GetMetadata(fileInode)
	if nil != err {
		t.Fatalf("GetMetadata(fileInode) failed: %v", err)
	}

	if !dirInodeMetadataAfterUnlink.CreationTime.Equal(dirInodeMetadataAfterMove.CreationTime) {
		t.Fatalf("dirInodeMetadataAfterUnlink.CreationTime unexpected")
	}
	if dirInodeMetadataAfterUnlink.ModificationTime.Before(timeAfterMove) || dirInodeMetadataAfterUnlink.ModificationTime.After(timeAfterUnlink) {
		t.Fatalf("dirInodeMetadataAfterUnlink.ModificationTime unexpected")
	}
	if !dirInodeMetadataAfterUnlink.AccessTime.Equal(dirInodeMetadataAfterMove.AccessTime) {
		t.Fatalf("dirInodeMetadataAfterUnlink.AccessTime unexpected change")
	}
	if dirInodeMetadataAfterUnlink.AttrChangeTime.Before(timeAfterMove) || dirInodeMetadataAfterUnlink.AttrChangeTime.After(timeAfterUnlink) {
		t.Fatalf("dirInodeMetadataAfterUnlink.AttrChangeTime unexpected")
	}
	if !fileInodeMetadataAfterUnlink.CreationTime.Equal(fileInodeMetadataAfterMove.CreationTime) {
		t.Fatalf("fileInodeMetadataAfterUnlink.CreationTime unexpected")
	}
	if !fileInodeMetadataAfterUnlink.ModificationTime.Equal(fileInodeMetadataAfterMove.ModificationTime) {
		t.Fatalf("fileInodeMetadataAfterUnlink.ModificationTime unexpected")
	}
	if !fileInodeMetadataAfterUnlink.AccessTime.Equal(fileInodeMetadataAfterMove.AccessTime) {
		t.Fatalf("fileInodeMetadataAfterUnlink.AccessTime unexpected")
	}
	if fileInodeMetadataAfterUnlink.AttrChangeTime.Before(timeAfterMove) || fileInodeMetadataAfterUnlink.AttrChangeTime.After(timeAfterUnlink) {
		t.Fatalf("fileInodeMetadataAfterUnlink.AttrChangeTime unexpected")
	}

	time.Sleep(positiveDurationToDelayOrSkew)

	err = testVolumeHandle.Write(fileInode, uint64(0), []byte{0x00}, nil)
	if nil != err {
		t.Fatalf("Write(fileInode, uint64(0), []byte{0x00}) failed: %v", err)
	}

	time.Sleep(positiveDurationToDelayOrSkew)

	timeAfterWrite := time.Now()

	fileInodeMetadataAfterWrite, err := testVolumeHandle.GetMetadata(fileInode)
	if nil != err {
		t.Fatalf("GetMetadata(fileInode) failed: %v", err)
	}

	if !fileInodeMetadataAfterWrite.CreationTime.Equal(fileInodeMetadataAfterUnlink.CreationTime) {
		t.Fatalf("fileInodeMetadataAfterWrite.CreationTime unexpected")
	}
	if fileInodeMetadataAfterWrite.ModificationTime.Before(timeAfterUnlink) || fileInodeMetadataAfterWrite.ModificationTime.After(timeAfterWrite) {
		t.Fatalf("fileInodeMetadataAfterWrite.ModificationTime unexpected")
	}
	if !fileInodeMetadataAfterWrite.AttrChangeTime.Equal(fileInodeMetadataAfterWrite.ModificationTime) {
		t.Fatalf("fileInodeMetadataAfterWrite.AttrChangeTime unexpected")
	}
	if !fileInodeMetadataAfterWrite.AccessTime.Equal(fileInodeMetadataAfterUnlink.AccessTime) {
		t.Fatalf("fileInodeMetadataAfterWrite.AccessTime unexpected change")
	}

	time.Sleep(positiveDurationToDelayOrSkew)

	objectPath, err := testVolumeHandle.ProvisionObject()
	if nil != err {
		t.Fatalf("ProvisionObject() failed: %v", err)
	}
	err = testVolumeHandle.Wrote(fileInode, objectPath, []uint64{0}, []uint64{0}, []uint64{2}, false)
	if nil != err {
		t.Fatalf("Wrote(fileInode, objectPath, []uint64{0}, []uint64{0}, []uint64{2}, false) failed: %v", err)
	}

	time.Sleep(positiveDurationToDelayOrSkew)

	timeAfterWrote := time.Now()

	fileInodeMetadataAfterWrote, err := testVolumeHandle.GetMetadata(fileInode)
	if nil != err {
		t.Fatalf("GetMetadata(fileInode) failed: %v", err)
	}

	if !fileInodeMetadataAfterWrote.CreationTime.Equal(fileInodeMetadataAfterWrite.CreationTime) {
		t.Fatalf("fileInodeMetadataAfterWrote.CreationTime unexpected")
	}
	if fileInodeMetadataAfterWrote.ModificationTime.Before(timeAfterWrite) || fileInodeMetadataAfterWrote.ModificationTime.After(timeAfterWrote) {
		t.Fatalf("fileInodeMetadataAfterWrote.ModificationTime unexpected")
	}
	if !fileInodeMetadataAfterWrote.AttrChangeTime.Equal(fileInodeMetadataAfterWrote.ModificationTime) {
		t.Fatalf("fileInodeMetadataAfterWrote.AttrChangeTime should equal fileInodeMetadataAfterWrote.ModificationTime")
	}
	if !fileInodeMetadataAfterWrote.AccessTime.Equal(fileInodeMetadataAfterWrite.AccessTime) {
		t.Fatalf("fileInodeMetadataAfterWrote.AccessTime unexpected change")
	}

	time.Sleep(positiveDurationToDelayOrSkew)

	err = testVolumeHandle.SetSize(fileInode, uint64(0))
	if nil != err {
		t.Fatalf("SetSize(fileInode, uint64(0)) failed: %v", err)
	}

	time.Sleep(positiveDurationToDelayOrSkew)

	timeAfterSetSize := time.Now()

	fileInodeMetadataAfterSetSize, err := testVolumeHandle.GetMetadata(fileInode)
	if nil != err {
		t.Fatalf("GetMetadata(fileInode) failed: %v", err)
	}

	if !fileInodeMetadataAfterSetSize.CreationTime.Equal(fileInodeMetadataAfterWrote.CreationTime) {
		t.Fatalf("fileInodeMetadataAfterSetSize.CreationTime unexpected")
	}
	if fileInodeMetadataAfterSetSize.ModificationTime.Before(timeAfterWrote) || fileInodeMetadataAfterSetSize.ModificationTime.After(timeAfterSetSize) {
		t.Fatalf("fileInodeMetadataAfterSetSize.ModificationTime unexpected")
	}
	if !fileInodeMetadataAfterSetSize.AttrChangeTime.Equal(fileInodeMetadataAfterSetSize.ModificationTime) {
		t.Fatalf("fileInodeMetadataAfterSetsize.AttrChangeTime should equal fileInodeMetadataAfterSetSize.ModificationTime")
	}
	if !fileInodeMetadataAfterSetSize.AccessTime.Equal(fileInodeMetadataAfterWrote.AccessTime) {
		t.Fatalf("fileInodeMetadataAfterSetSize.AccessTime unexpected change")
	}

	// TODO: Need to test GetFragmentationReport()

	// TODO: Once implemented, need to test Optimize()

	testTeardown(t)
}

func TestInodeDiscard(t *testing.T) {
	testSetup(t, false)

	assert := assert.New(t)
	testVolumeHandle, err := FetchVolumeHandle("TestVolume")
	if nil != err {
		t.Fatalf("FetchVolumeHandle(\"TestVolume\") should have worked - got error: %v", err)
	}

	// Calculate how many inodes we must create to make sure the inode cache discard
	// routine will find something to discard.
	vS := testVolumeHandle.(*volumeStruct)
	maxBytes := vS.inodeCacheLRUMaxBytes
	iSize := globals.inodeSize
	entriesNeeded := maxBytes / iSize
	entriesNeeded = entriesNeeded * 6
	for i := uint64(0); i < entriesNeeded; i++ {
		fileInodeNumber, err := testVolumeHandle.CreateFile(InodeMode(0000), InodeRootUserID, InodeGroupID(0))
		if nil != err {
			t.Fatalf("CreateFile() failed: %v", err)
		}

		fName := fmt.Sprintf("file-%v i: %v", fileInodeNumber, i)
		err = testVolumeHandle.Link(RootDirInodeNumber, fName, fileInodeNumber, false)
		if nil != err {
			t.Fatalf("Link(RootDirInodeNumber, \"%v\", file1Inode, false) failed: %v", fName, err)
		}

		fileInode, ok, err := vS.fetchInode(fileInodeNumber)
		assert.Nil(err, nil, "Unable to fetchInode due to err - even though just created")
		assert.True(ok, "fetchInode returned !ok - even though just created")
		assert.False(fileInode.dirty, "fetchInode.dirty == true - even though just linked")
	}

	discarded, dirty, locked, lruItems := vS.inodeCacheDiscard()

	assert.NotEqual(discarded, uint64(0), "Number of inodes discarded should be non-zero")
	assert.Equal(dirty, uint64(0), "Number of inodes dirty should zero")
	assert.Equal(locked, uint64(0), "Number of inodes locked should zero")
	assert.Equal((lruItems * iSize), (maxBytes/iSize)*iSize, "Number of inodes in cache not same as max")

	testTeardown(t)
}
