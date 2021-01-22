// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package pfsagentConfig

import "fmt"

type stringStack struct {
	items []string
}

type stringArrayStack struct {
	items [][]string
}

type stringMapStack struct {
	items []map[string]string
}

func (s stringStack) push(val string) (err error) {
	if len(val) == 0 {
		err = fmt.Errorf("value cannot be zero-length")
	} else {
		s.items = append(s.items, val)
	}
	return
}

func (s stringStack) pop() (val string) {
	if len(s.items) == 0 {
		return
	}
	val = s.peek()
	s.items = s.items[:len(s.items)-2]

	return
}

func (s stringStack) peek() (val string) {
	val = s.items[len(s.items)-1]
	return
}

func (s stringArrayStack) push(val []string) (err error) {
	if len(val) == 0 {
		err = fmt.Errorf("value cannot be zero-length")
	} else {
		s.items = append(s.items, val)
	}
	return
}

func (s stringArrayStack) pop() (val []string) {
	if len(s.items) == 0 {
		return
	}
	val = s.peek()
	s.items = s.items[:len(s.items)-2]

	return
}

func (s stringArrayStack) peek() (val []string) {
	val = s.items[len(s.items)-1]
	return
}

func (s stringMapStack) push(val map[string]string) (err error) {
	if len(val) == 0 {
		err = fmt.Errorf("value cannot be zero-length")
	} else {
		s.items = append(s.items, val)
	}
	return
}

func (s stringMapStack) pop() (val map[string]string) {
	if len(s.items) == 0 {
		return
	}
	val = s.peek()
	s.items = s.items[:len(s.items)-2]

	return
}

func (s stringMapStack) peek() (val map[string]string) {
	val = s.items[len(s.items)-1]
	return
}
