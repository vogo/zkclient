// Copyright 2019 The vogo Authors. All rights reserved.
// author: wongoo
// since: 2018/12/21
//

package zkclient

import (
	"errors"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/vogo/logger"
)

// EventHandler zookeeper event handler
type EventHandler func(*Client, *zk.Event) (<-chan zk.Event, error)

// EventCallback event callback
type EventCallback func() error

// Watcher zookeeper watcher
type Watcher struct {
	Path    string
	client  *Client
	handler EventHandler
}

// NewWatcher create new watcher
func (cli *Client) NewWatcher(path string, handler EventHandler) (*Watcher, error) {
	if handler == nil {
		return nil, errors.New("nil handler")
	}
	return &Watcher{Path: path, client: cli, handler: handler}, nil
}

// Watch start watch event
func (w *Watcher) Watch() {
	go func() {
		logger.Debugf("start zk watcher: %s", w.Path)

		var evt *zk.Event
		var ch <-chan zk.Event
		var err error

		for {
			select {
			case <-w.client.close:
				return
			default:
				ch, err = w.handler(w.client, evt)
				if err != nil {
					logger.Errorf("zk watcher handle error %s: %v", w.Path, err)
					w.client.AppendDeadWatcher(w)
					break // exit watching
				}

				if ch == nil {
					// return nil chan to exit watcher
					break
				}

				event := <-ch
				evt = &event
				logger.Debugf("zk watcher new event %s: %v", w.Path, evt)

				if !StateAlive(evt.State) {
					w.client.AppendDeadWatcher(w)
					break // exit watching
				}
			}
		}
	}()
}
