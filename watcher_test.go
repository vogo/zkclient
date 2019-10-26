// Copyright 2018-2019 The vogo Authors. All rights reserved.
// author: wongoo
// since: 2019/10/18

package zkclient

import (
	"errors"
	"testing"
)

func TestWatcherClose(t *testing.T) {
	c1, w, err := createClientWatcher(t)
	if err != nil {
		return
	}

	waitEventWatch()

	c1.Close()
	w.Close()

	c1, w, err = createClientWatcher(t)
	if err != nil {
		return
	}

	waitEventWatch()

	w.Close()
	c1.Close()
}

func createClientWatcher(t *testing.T) (*Client, *Watcher, error) {
	c1 := connectLocalZK(t)
	if c1 == nil {
		return nil, nil, errors.New("can't conn zk")
	}

	path := "/test/watcher_close_users"
	users := make(map[string]*user)
	w, err := c1.SyncWatchJSONMap(path, users, true, &mListener{})

	return c1, w, err
}
