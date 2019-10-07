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
