// Copyright 2011 Evan Shaw. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build darwin dragonfly freebsd linux openbsd solaris netbsd

package mmap

import (
	"syscall"
	"unsafe"
)

var _zero uintptr

func mmap(fd, inProt, inFlags uintptr, offset int64, length int) (MMap, error) {
	flags := syscall.MAP_SHARED
	prot := syscall.PROT_READ
	switch {
	case inProt&COPY != 0:
		prot |= syscall.PROT_WRITE
		flags = syscall.MAP_PRIVATE
	case inProt&RDWR != 0:
		prot |= syscall.PROT_WRITE
	}
	if inProt&EXEC != 0 {
		prot |= syscall.PROT_EXEC
	}
	if inFlags&ANON != 0 {
		flags |= syscall.MAP_ANON
	}
	data, err := syscall.Mmap(int(fd), offset, length, prot, flags)
	if err != nil {
		return nil, err
	}
	return *(*MMap)(unsafe.Pointer(&data)), nil
}

func (m MMap) flush() error {
	var _p0 unsafe.Pointer
	if len(m) > 0 {
		_p0 = unsafe.Pointer(&m[0])
	} else {
		_p0 = unsafe.Pointer(&_zero)
	}
	_, _, e1 := syscall.Syscall(syscall.SYS_MSYNC, uintptr(_p0), uintptr(len(m)), uintptr(syscall.MS_SYNC))
	if e1 != 0 {
		return e1
	}
	return nil
}

func (m MMap) lock() error {
	var _p0 unsafe.Pointer
	if len(m) > 0 {
		_p0 = unsafe.Pointer(&m[0])
	} else {
		_p0 = unsafe.Pointer(&_zero)
	}
	_, _, e1 := syscall.Syscall(syscall.SYS_MLOCK, uintptr(_p0), uintptr(len(m)), 0)
	if e1 != 0 {
		return e1
	}
	return nil
}

func (m MMap) unlock() error {
	var _p0 unsafe.Pointer
	if len(m) > 0 {
		_p0 = unsafe.Pointer(&m[0])
	} else {
		_p0 = unsafe.Pointer(&_zero)
	}
	_, _, e1 := syscall.Syscall(syscall.SYS_MUNLOCK, uintptr(_p0), uintptr(len(m)), 0)
	if e1 != 0 {
		return e1
	}
	return nil
}

func (m MMap) unmap() error {
	b := *(*[]byte)(unsafe.Pointer(&m))
	return syscall.Munmap(b)
}
