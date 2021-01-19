// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package transitions

import (
	"github.com/swiftstack/ProxyFS/conf"
)

// Callbacks is the interface implemented by each package desiring notification of
// configuration changes. Each such package should implement a struct with pointer
// receivers for each API listed below even when there is no interest in being
// notified of a particular condition.
//
// By calling transitions.Register() in the package's init() func, the proper order
// of registration will be ensured. In specific, the following callbacks will be
// issued in the same order as package init() func calls have registered:
//
//   Up()
//   VolumeGroupCreated()
//   VolumeGroupMoved()
//   VolumeCreated()
//   VolumeMoved()
//   ServeVolume()
//   SignaledFinish()
//
// By contrast, the following callbacks will be issued in the reverse order as package
// init() func calls have registered:
//
//   SignaledStart()
//   UnserveVolume()
//   VolumeDestroyed()
//   VolumeGroupDestroyed()
//   Down()
//
type Callbacks interface {
	Up(confMap conf.ConfMap) (err error)
	VolumeGroupCreated(confMap conf.ConfMap, volumeGroupName string, activePeer string, virtualIPAddr string) (err error)
	VolumeGroupMoved(confMap conf.ConfMap, volumeGroupName string, activePeer string, virtualIPAddr string) (err error)
	VolumeGroupDestroyed(confMap conf.ConfMap, volumeGroupName string) (err error)
	VolumeCreated(confMap conf.ConfMap, volumeName string, volumeGroupName string) (err error)
	VolumeMoved(confMap conf.ConfMap, volumeName string, volumeGroupName string) (err error)
	VolumeDestroyed(confMap conf.ConfMap, volumeName string) (err error)
	ServeVolume(confMap conf.ConfMap, volumeName string) (err error)
	UnserveVolume(confMap conf.ConfMap, volumeName string) (err error)
	VolumeToBeUnserved(confMap conf.ConfMap, volumeName string) (err error)
	SignaledStart(confMap conf.ConfMap) (err error)
	SignaledFinish(confMap conf.ConfMap) (err error)
	Down(confMap conf.ConfMap) (err error)
}

// Register should be called from a package's init() func should the package be interested
// in one or more of the callbacks that they will receive. Each callback func should receive
// a struct implementing the Callbacks interface by reference.
//
// As an example, consider the following:
//
//   package foo
//
//   import "github.com/swiftstack/ProxyFS/conf"
//   import "github.com/swiftstack/ProxyFS/transitions"
//
//   type transitionsCallbackInterfaceStruct struct {
//   }
//
//   var transitionsCallbackInterface transitionsCallbackInterfaceStruct
//
//   func init() {
//       transitions.Register("foo", &transitionsCallbackInterface)
//   }
//
//   func (transitionsCallbackInterface *transitionsCallbackInterfaceStruct) Up(confMap conf.ConfMap) (err error) {
//       // Perform start-up initialization derived from confMap
//       // ...set err at some point
//       return
//   }
//
//   ...
//
// Package foo would also have to provide callbacks for each of the other APIs in
// the transitions.Callbacks interface (returning nil if simply not interested).
//
// A special exception to the need for registration is the package logger. Package
// transitions makes an explicit reference to logging functions in package logger and,
// as such, will perform the registration for package logger itself.
//
func Register(packageName string, callbacks Callbacks) {
	register(packageName, callbacks)
}

// Up should be called at startup by the main() (or setup func) of each program including
// any of the packages needing callback notifications. This will trigger Up() callbacks
// to each of the packages that have registered with package transitions starting with
// package logger (that was registered automatically by package transitions).
//
// Following the Up() callbacks, the following subset of the callbacks triggered
// by a call to Signaled() will be made as if the prior confMap were empty:
//
//   VolumeGroupCreated() - registration order (for each such volume group)
//   VolumeCreated()      - registration order (for each such volume)
//   ServeVolume()        - registration order (for each such volume)
//   SignaledFinish()     - registration order
//
func Up(confMap conf.ConfMap) (err error) {
	return up(confMap)
}

// Signaled should be called during execution of a signal handler for e.g. SIGHUP by the
// main() (or monitoring func) of each program including any of the packages needing
// callback notifications. This will potentially trigger multiple of the following
// callbacks to each of the packages that have registered with package transitions.
//
// As part of this call, a determination will be made as to which volumes have migrated
// as well as which volumes have either been created or destroyed. Upon determining
// these volume sets, the following callbacks will be issued to each of the packages
// that have registered with package transitions:
//
//   VolumeToBeUnserved()   - reverse registration order (for each such volume)
//   SignaledStart()        - reverse registration order
//   VolumeGroupCreated()   -         registration order (for each such volume group)
//   VolumeCreated()        -         registration order (for each such volume)
//   UnserveVolume()        - reverse registration order (for each such volume)
//   VolumeGroupMoved()     -         registration order (for each such volume group)
//   VolumeMoved()          -         registration order (for each such volume)
//   ServeVolume()          -         registration order (for each such volume)
//   VolumeDestroyed()      - reverse registration order (for each such volume)
//   VolumeGroupDestroyed() - reverse registration order (for each such volume group)
//   SignaledFinish()       -         registration order
//
func Signaled(confMap conf.ConfMap) (err error) {
	return signaled(confMap)
}

// Down should be called just before shutdown by the main() (or teardown func) of each
// program including any of the packages needing callback notifications. This will trigger
// Down() callbacks to each of the packages that have registered with package transitions
// ending with package logger (that was registered automatically by package transitions).
//
// Prior to the Down() callbacks, the following subset of the callbacks triggered
// by a call to Signaled() will be made as if the prior confMap were empty:
//
//   VolumeToBeUnserved()   - reverse registration order (for each such volume)
//   SignaledStart()        - reverse registration order
//   UnserveVolume()        - reverse registration order (for each such volume)
//   VolumeDestroyed()      - reverse registration order (for each such volume)
//   VolumeGroupDestroyed() - reverse registration order (for each such volume group)
//
func Down(confMap conf.ConfMap) (err error) {
	return down(confMap)
}

// UpgradeConfMapIfNeeded should be removed once backwards compatibility is no longer required...
//
func UpgradeConfMapIfNeeded(confMap conf.ConfMap) (err error) {
	return upgradeConfMapIfNeeded(confMap)
}
