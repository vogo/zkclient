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

type MapHandler struct {
	path      string
	lock      sync.Mutex
	value     reflect.Value
	typ       reflect.Type
	syncChild bool
	codec     Codec
	listener  ChildListener
	children  map[string]struct{}
}

func NewMapHandler(path string, obj interface{}, syncChild bool, codec Codec, h ChildListener) (*MapHandler, error) {
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

	return &MapHandler{
		path:      path,
		value:     reflect.ValueOf(obj),
		typ:       valueTyp,
		syncChild: syncChild,
		codec:     codec,
		lock:      sync.Mutex{},
		listener:  h,
		children:  make(map[string]struct{}),
	}, nil
}

func (h *MapHandler) Encode(key string) ([]byte, error) {
	v := h.value.MapIndex(reflect.ValueOf(key))
	if v.IsNil() {
		return nil, io.EOF
	}

	return h.codec.Encode(v.Interface())
}

func (h *MapHandler) Decode(key string, data []byte) error {
	h.lock.Lock()
	defer h.lock.Unlock()

	v, err := h.codec.Decode(data, h.typ)
	if err != nil {
		return err
	}

	h.value.SetMapIndex(reflect.ValueOf(key), v)

	if h.listener != nil {
		go func() {
			h.listener(h.path, key, v.Interface())
		}()
	}

	return nil
}

func (h *MapHandler) Delete(key string) {
	h.lock.Lock()
	defer h.lock.Unlock()

	h.value.SetMapIndex(reflect.ValueOf(key), reflect.Value{})

	if h.listener != nil {
		go func() {
			h.listener(h.path, key, nil)
		}()
	}
}

func (h *MapHandler) Path() string {
	return h.path
}

func (h *MapHandler) Handle(w *Watcher, evt *zk.Event) (<-chan zk.Event, error) {
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
	path       string
	mapHandler *MapHandler
}

func (ch *childHandler) Path() string {
	return ch.path
}

func (ch *childHandler) Handle(w *Watcher, evt *zk.Event) (<-chan zk.Event, error) {
	if evt != nil && evt.Type == zk.EventNodeDeleted {
		return nil, nil // return nil chan to exit watching
	}

	return ch.mapHandler.handleChild(w.client, ch.path)
}

func (h *MapHandler) syncWatchChild(w *Watcher, child string) {
	childPath := h.path + "/" + child

	if !h.syncChild {
		if _, err := h.handleChild(w.client, childPath); err != nil {
			logger.Errorf("zk load map child error: %v", err)
		}

		return
	}

	childWatcher := w.newChildWatcher(&childHandler{path: childPath, mapHandler: h})
	childWatcher.Watch()
}

// handleChild load map child value into packMap, and return the event chan for waiting the next event
func (h *MapHandler) handleChild(client *Client, childPath string) (<-chan zk.Event, error) {
	var (
		data []byte
		err  error
		ch   <-chan zk.Event
	)

	logger.Debugf("zk read node [%s]", childPath)

	if h.syncChild {
		data, _, ch, err = client.Conn().GetW(childPath)
		if err != nil {
			return nil, err
		}
	} else {
		data, _, err = client.Conn().Get(childPath)
		if err != nil {
			return nil, err
		}
	}

	if err = h.Decode(filepath.Base(childPath), data); err != nil {
		if err != io.EOF {
			logger.Warnf("zk failed to parse %s: %v", childPath, err)
		}

		return ch, nil
	}

	return ch, nil
}
