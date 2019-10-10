// Copyright 2019 The vogo Authors. All rights reserved.
// author: wongoo
// since: 2018/12/27
//

package zkclient

import "github.com/samuel/go-zookeeper/zk"

// StateAlive whether zk state alive
func StateAlive(state zk.State) bool {
	switch state {
	case zk.StateDisconnected, zk.StateAuthFailed, zk.StateConnecting:
		return false
	}

	return true
}

func IsZKInvalidErr(err error) bool {
	switch err {
	case zk.ErrInvalidACL, zk.ErrInvalidPath:
		return true
	default:
		return false
	}
}

func IsZKRecoverableErr(err error) bool {
	switch err {
	case zk.ErrClosing, zk.ErrConnectionClosed, zk.ErrSessionExpired, zk.ErrSessionMoved:
		return true
	default:
		return false
	}
}
