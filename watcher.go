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

// EventHandler zookeeper event listener
type EventHandler interface {
	Handle(*Watcher, *zk.Event) (<-chan zk.Event, error)
}

// ValueListener node watch listener
type ValueListener func(interface{})

// ChildListener child watch listener
type ChildListener func(child string, obj interface{})

// Watcher zookeeper watcher
type Watcher struct {
	client  *Client
	handler EventHandler
	close   chan struct{}
	Path    string
}

// NewWatcher create new watcher
func (cli *Client) NewWatcher(path string, handler EventHandler) (*Watcher, error) {
	if handler == nil {
		return nil, errors.New("nil listener")
	}

	return &Watcher{
		client:  cli,
		handler: handler,
		close:   make(chan struct{}),
		Path:    path,
	}, nil
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
			case <-w.close:
				return
			default:
				if evt != nil && evt.Type == zk.EventNodeDeleted {
					// return nil chan to exit watcher
					return
				}

				ch, err = w.handler.Handle(w, evt)
				if err != nil {
					logger.Errorf("zk watcher handle error %s: %v", w.Path, err)

					if IsZKRecoverableErr(err) {
						w.client.AppendDeadWatcher(w)
					}

					return // exit watching
				}

				if ch == nil {
					// return nil chan to exit watcher
					return
				}

				event := <-ch
				evt = &event
				logger.Debugf("zk watcher new event %s: %v", w.Path, evt)

				if !StateAlive(evt.State) {
					w.client.AppendDeadWatcher(w)
					return // exit watching
				}
			}
		}
	}()
}

func (w *Watcher) newChildWatcher(child string, handler EventHandler) *Watcher {
	return &Watcher{
		client:  w.client,
		handler: handler,
		close:   w.close,
		Path:    w.Path + "/" + child,
	}
}
