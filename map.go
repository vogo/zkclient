// Copyright 2019 The vogo Authors. All rights reserved.
// author: wongoo
// since: 2018/12/27
//

package zkclient

import (
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"sync"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/vogo/logger"
)

type mapHandler struct {
	path        string
	lock        sync.Mutex
	value       reflect.Value
	typ         reflect.Type
	codec       Codec
	syncChild   bool
	listenAsync bool
	listener    ChildListener
	children    map[string]struct{}
}

func (cli *Client) newMapHandler(path string, obj interface{}, syncChild bool, codec Codec,
	watchOnly bool, listener ChildListener) (*mapHandler, error) {
	if path == "" {
		return nil, errors.New("path required")
	}

	typ := reflect.TypeOf(obj)
	if typ.Kind() != reflect.Map {
		return nil, errors.New("map object required")
	}

	if typ.Key().Kind() != reflect.String {
		return nil, errors.New("string map key required")
	}

	valueTyp := typ.Elem()

	if valueTyp.Kind() != reflect.Ptr {
		return nil, errors.New("pointer value required")
	}

	if valueTyp.Elem().Kind() == reflect.Ptr {
		return nil, errors.New("not support multiple level pointer value")
	}

	if codec == nil {
		return nil, errors.New("codec required")
	}

	if watchOnly && listener == nil {
		return nil, errors.New("listener required when watch only")
	}

	handler := &mapHandler{
		path:        path,
		typ:         valueTyp,
		syncChild:   syncChild,
		codec:       codec,
		lock:        sync.Mutex{},
		listenAsync: cli.listenAsync,
		listener:    listener,
		children:    make(map[string]struct{}),
	}

	if !watchOnly {
		handler.value = reflect.ValueOf(obj)
	}

	return handler, nil
}

func (h *mapHandler) Encode(key string) ([]byte, error) {
	if h.value == nilValue {
		return nil, io.EOF
	}

	v := h.value.MapIndex(reflect.ValueOf(key))
	if v.IsNil() {
		return nil, io.EOF
	}

	return h.codec.Encode(v.Interface())
}

func (h *mapHandler) Decode(stat *zk.Stat, key string, data []byte) error {
	h.lock.Lock()
	defer h.lock.Unlock()

	v, err := h.codec.Decode(data, h.typ)
	if err != nil {
		return err
	}

	if h.value != nilValue {
		h.value.SetMapIndex(reflect.ValueOf(key), v)
	}

	if h.listener != nil {
		f := func() {
			h.listener.Update(h.path, key, stat, v.Interface())
		}

		if h.listenAsync {
			go f()
		} else {
			f()
		}
	}

	return nil
}

func (h *mapHandler) Delete(key string) {
	h.lock.Lock()
	defer h.lock.Unlock()

	if h.value != nilValue {
		h.value.SetMapIndex(reflect.ValueOf(key), reflect.Value{})
	}

	if h.listener != nil {
		f := func() {
			h.listener.Delete(h.path, key)
		}

		if h.listenAsync {
			go f()
		} else {
			f()
		}
	}
}

func (h *mapHandler) Path() string {
	return h.path
}

func (h *mapHandler) Handle(w *Watcher, evt *zk.Event) (<-chan zk.Event, error) {
	if evt != nil && evt.Type == zk.EventNodeDeleted {
		logger.Infof("zk watcher [%s] node deleted", h.path)

		for child := range h.children {
			h.Delete(child)
		}

		return nil, nil
	}

	children, _, wch, err := w.client.Conn().ChildrenW(h.path)
	if err != nil {
		if err == zk.ErrNoNode {
			_ = w.client.EnsurePath(h.path)
			children, _, wch, err = w.client.Conn().ChildrenW(h.path)
		}

		if err != nil {
			return nil, err
		}
	}

	newChildren := make(map[string]struct{})
	oldChildren := h.children

	for _, child := range children {
		newChildren[child] = nilStruct

		if _, ok := oldChildren[child]; !ok {
			h.syncWatchChild(w, child)
		}
	}

	for child := range oldChildren {
		if _, ok := newChildren[child]; !ok {
			logger.Infof("zk delete sub node [%s] of [%s]", child, h.path)
			h.Delete(child)
		}
	}

	h.children = newChildren

	return wch, nil
}

type childHandler struct {
	path    string
	handler *mapHandler
}

func (ch *childHandler) Path() string {
	return ch.path
}

func (ch *childHandler) Handle(w *Watcher, evt *zk.Event) (<-chan zk.Event, error) {
	if evt != nil && evt.Type == zk.EventNodeDeleted {
		return nil, nil // return nil chan to exit watching
	}

	return ch.handler.handleChild(w.client, ch.path)
}

func (h *mapHandler) syncWatchChild(w *Watcher, child string) {
	childPath := h.path + "/" + child

	if !h.syncChild {
		if _, err := h.handleChild(w.client, childPath); err != nil {
			logger.Errorf("zk load map child error: %v", err)
		}

		return
	}

	childWatcher := w.newChildWatcher(&childHandler{path: childPath, handler: h})
	childWatcher.Watch()
}

// handleChild load map child value into packMap, and return the event chan for waiting the next event
func (h *mapHandler) handleChild(client *Client, childPath string) (<-chan zk.Event, error) {
	var (
		data []byte
		err  error
		ch   <-chan zk.Event
		stat *zk.Stat
	)

	logger.Debugf("zk read node [%s]", childPath)

	if h.syncChild {
		data, stat, ch, err = client.Conn().GetW(childPath)
		if err != nil {
			return nil, err
		}
	} else {
		data, stat, err = client.Conn().Get(childPath)
		if err != nil {
			return nil, err
		}
	}

	if err = h.Decode(stat, filepath.Base(childPath), data); err != nil {
		if err != io.EOF {
			logger.Warnf("zk failed to parse %s: %v", childPath, err)
		}

		return ch, nil
	}

	return ch, nil
}
