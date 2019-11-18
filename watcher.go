// Copyright 2018-2019 The vogo Authors. All rights reserved.
// author: wongoo
// since: 2018/12/21
//

package zkclient

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/vogo/logger"
)

// EventHandler zookeeper event listener
type EventHandler interface {
	Path() string
	Handle(*Watcher, *zk.Event) (<-chan zk.Event, error)
}

// ValueListener node watch listener
type ValueListener interface {
	Update(path string, stat *zk.Stat, obj interface{})
	Delete(path string)
}

// ChildListener child watch listener
type ChildListener interface {
	Update(path, child string, stat *zk.Stat, obj interface{})
	Delete(path, child string)
}

// Watcher zookeeper watcher
type Watcher struct {
	sync.Mutex
	client  *Client
	handler EventHandler
	done    chan struct{}
	alive   int32
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
		alive:   0,
	}, nil
}

// Done chan
func (w *Watcher) Done() <-chan struct{} {
	return w.done
}

// Alive whether watcher is watching
func (w *Watcher) Alive() bool {
	return atomic.LoadInt32(&w.alive) == 1
}

// Close close watch event
func (w *Watcher) Close() {
	w.Lock()
	defer w.Unlock()

	select {
	case <-w.done:
	default:
		close(w.done)
	}
}

// Watch start watch event
func (w *Watcher) Watch() {
	go func() {
		path := w.handler.Path()
		logger.Debugf("zk watcher [%s] start", path)

		atomic.StoreInt32(&w.alive, 1)
		defer atomic.StoreInt32(&w.alive, 0)

		var (
			evt *zk.Event
			ch  <-chan zk.Event
			err error
		)

		for {
			ch, err = w.handler.Handle(w, evt)
			if err != nil {
				logger.Errorf("zk watcher [%s] handle error: %v", path, err)

				if IsZKRecoverableErr(err) {
					w.client.AppendDeadWatcher(w)
				}

				return // exit watching
			}

			if ch == nil {
				logger.Debugf("zk watcher [%s] exit", path)

				// return nil chan to exit watcher
				return
			}

			select {
			case <-w.client.done:
				logger.Debugf("zk watcher [%s] exit for client closed", path)
				w.Close()

				return
			case <-w.done:
				logger.Debugf("zk watcher [%s] exit for watcher closed", path)
				return
			case event := <-ch:
				evt = &event
				logger.Debugf("zk watcher [%s] new event: %v", path, evt)

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
