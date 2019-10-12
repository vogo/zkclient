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
	Path() string
	Handle(*Watcher, *zk.Event) (<-chan zk.Event, error)
}

// ValueListener node watch listener
type ValueListener func(path string, obj interface{})

// ChildListener child watch listener
type ChildListener func(path, child string, obj interface{})

// Watcher zookeeper watcher
type Watcher struct {
	client  *Client
	handler EventHandler
	done    chan struct{}
}

// NewWatcher create new watcher
func (cli *Client) NewWatcher(handler EventHandler) (*Watcher, error) {
	if handler == nil {
		return nil, errors.New("nil listener")
	}

	return &Watcher{
		client:  cli,
		handler: handler,
		done:    make(chan struct{}),
	}, nil
}

// Close close watch event
func (w *Watcher) Close() {
	close(w.done)
}

// Watch start watch event
func (w *Watcher) Watch() {
	go func() {
		path := w.handler.Path()
		logger.Debugf("start zk watcher: %s", path)

		var (
			evt *zk.Event
			ch  <-chan zk.Event
			err error
		)

		for {
			if evt != nil && evt.Type == zk.EventNodeDeleted {
				// return nil chan to exit watcher
				return
			}

			ch, err = w.handler.Handle(w, evt)
			if err != nil {
				logger.Errorf("zk watcher handle error %s: %v", path, err)

				if IsZKRecoverableErr(err) {
					w.client.AppendDeadWatcher(w)
				}

				return // exit watching
			}

			if ch == nil {
				// return nil chan to exit watcher
				return
			}

			select {
			case <-w.client.done:
				return
			case <-w.done:
				return
			case event := <-ch:
				evt = &event
				logger.Debugf("zk watcher new event %s: %v", path, evt)

				if !StateAlive(evt.State) {
					w.client.AppendDeadWatcher(w)
					return // exit watching
				}
			}
		}
	}()
}

func (w *Watcher) newChildWatcher(handler EventHandler) *Watcher {
	return &Watcher{
		client:  w.client,
		handler: handler,
		done:    w.done,
	}
}
