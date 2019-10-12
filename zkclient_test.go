// Copyright 2019 The vogo Authors. All rights reserved.
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
			logger.Errorf("can't connect zookeeper")
			return
		}

		time.Sleep(time.Second)
		if testClient.ConnAlive() {
			atomic.StoreInt32(&localZKAlive, 1)
		} else {
			logger.Errorf("zookeeper not alive")
			testClient.Close()
		}
	})

	alive := atomic.LoadInt32(&localZKAlive)

	return alive == 1
}

func connectLocalZK(_ *testing.T) *Client {
	c, err := NewClient([]string{"127.0.0.1:2181"}, time.Second)
	if err != nil {
		logger.Errorf("can't connect zookeeper: %v", err)
		return nil
	}

	return c
}