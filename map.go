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
	lock      sync.Mutex
	value     reflect.Value
	typ       reflect.Type
	syncChild bool
	codec     Codec
	listener  ChildListener
	children  map[string]struct{}
}

func NewMapHandler(obj interface{}, syncChild bool, codec Codec, h ChildListener) (*MapHandler, error) {
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
		value:     reflect.ValueOf(obj),
		typ:       valueTyp,
		syncChild: syncChild,
		codec:     codec,
		lock:      sync.Mutex{},
		listener:  h,
		children:  make(map[string]struct{}),
	}, nil
}

func (h *MapHandler) Get(key string) ([]byte, error) {
	v := h.value.MapIndex(reflect.ValueOf(key))
	if v.IsNil() {
		return nil, io.EOF
	}
	return h.codec.Encode(v.Interface())
}

func (h *MapHandler) Set(key string, data []byte) error {
	h.lock.Lock()
	defer h.lock.Unlock()

	v, err := h.codec.Decode(data, h.typ)
	if err != nil {
		return err
	}
	h.value.SetMapIndex(reflect.ValueOf(key), v)

	if h.listener != nil {
		go func() {
			h.listener(key, v.Interface())
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
			h.listener(key, nil)
		}()
	}
}

func (h *MapHandler) Handle(w *Watcher, evt *zk.Event) (<-chan zk.Event, error) {
	children, _, wch, err := w.client.Conn().ChildrenW(w.Path)
	if err != nil {
		if err == zk.ErrNoNode {
			_ = w.client.EnsurePath(w.Path)
			children, _, wch, err = w.client.Conn().ChildrenW(w.Path)
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
			h.syncMapChild(w, w.Path+"/"+child)
		}
	}

	for child := range oldChildren {
		if _, ok := newChildren[child]; !ok {
			logger.Infof("zk delete path: %s/%s", w.Path, child)
			h.Delete(child)
		}
	}

	h.children = newChildren

	return wch, nil
}

type childHandler struct {
	mapHandler *MapHandler
}

func (ch *childHandler) Handle(w *Watcher, evt *zk.Event) (<-chan zk.Event, error) {
	if evt != nil && evt.Type == zk.EventNodeDeleted {
		return nil, nil // return nil chan to exit watching
	}

	return ch.mapHandler.loadMapChild(w, w.Path)
}

func (h *MapHandler) syncMapChild(w *Watcher, childPath string) {
	if !h.syncChild {
		_, err := h.loadMapChild(w, childPath)
		if err != nil {
			logger.Errorf("load zk map child error: %v", err)
		}
		return
	}

	childWatcher := w.newChildWatcher(childPath, &childHandler{h})
	childWatcher.Watch()
}

// loadMapChild load map child value into packMap, and return the event chan for waiting the next event
func (h *MapHandler) loadMapChild(w *Watcher, childPath string) (<-chan zk.Event, error) {
	var data []byte
	var err error
	var ch <-chan zk.Event

	logger.Debugf("read path --> %s", childPath)
	if h.syncChild {
		data, _, ch, err = w.client.Conn().GetW(childPath)
		if err != nil {
			return nil, err
		}
	} else {
		data, _, err = w.client.Conn().Get(childPath)
		if err != nil {
			return nil, err
		}
	}

	child := filepath.Base(childPath)
	err = h.Set(child, data)

	if err != nil {
		if err != io.EOF {
			logger.Warnf("failed to parse %s: %v", childPath, err)
		}
		return ch, nil
	}

	return ch, nil
}
