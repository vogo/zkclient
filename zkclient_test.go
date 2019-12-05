// Copyright 2018-2019 The vogo Authors. All rights reserved.
// author: wongoo
// since: 2019/10/12
//

package zkclient

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vogo/logger"
)

var (
	once         sync.Once
	localZKAlive int32
	testClient   *Client
)

func isLocalZKAlive(t *testing.T) bool {
	once.Do(func() {
		logger.SetLevel(logger.LevelDebug)

		testClient = connectLocalZK(t)
		if testClient == nil {
			logger.Errorf("zk can't connect")
			return
		}

		time.Sleep(time.Second)
		if testClient.ConnAlive() {
			atomic.StoreInt32(&localZKAlive, 1)
		} else {
			logger.Errorf("zk not alive")
			testClient.Close()
		}
	})

	alive := atomic.LoadInt32(&localZKAlive)

	return alive == 1
}

func connectLocalZK(_ *testing.T) *Client {
	c := NewClient([]string{"127.0.0.1:2181"})
	if c.ConnAlive() {
		logger.Errorf("zk can't connect")
		return nil
	}

	return c
}

const (
	watchWaitInterval = time.Second * 2
)

func waitEventWatch() {
	time.Sleep(watchWaitInterval)
}
