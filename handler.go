// Copyright 2019 The vogo Authors. All rights reserved.
// author: wongoo
// since: 2018/12/27
//

package zkclient

import (
	"errors"
	"io"
	"reflect"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/vogo/logger"
)

type ValueHandler struct {
	value    reflect.Value
	typ      reflect.Type
	codec    Codec
	listener WatchListener
}

func StringValueHandler(s *string, listener WatchListener) *ValueHandler {
	return &ValueHandler{
		value:    reflect.ValueOf(s),
		typ:      reflect.TypeOf(s),
		codec:    stringCodec,
		listener: listener,
	}
}

func NewValueHandler(obj interface{}, codec Codec, listener WatchListener) (*ValueHandler, error) {
	typ := reflect.TypeOf(obj)
	if typ.Kind() != reflect.Ptr {
		return nil, errors.New("pointer object required")
	}
	if typ.Elem().Kind() == reflect.Ptr {
		return nil, errors.New("not support multiple level pointer object")
	}
	if codec == nil {
		return nil, errors.New("codec required")
	}
	return &ValueHandler{
		value:    reflect.ValueOf(obj),
		typ:      typ,
		codec:    codec,
		listener: listener,
	}, nil
}

func (p *ValueHandler) Get() ([]byte, error) {
	return p.codec.Encode(p.value.Interface())
}

func (p *ValueHandler) Set(data []byte) error {
	v, err := p.codec.Decode(data, p.typ)
	if err != nil {
		return err
	}
	p.value.Elem().Set(v.Elem())

	if p.listener != nil {
		go func() {
			p.listener(p.value.Interface())
		}()
	}
	return nil
}

func (p *ValueHandler) Handle(w *Watcher, evt *zk.Event) (<-chan zk.Event, error) {
	data, _, wch, zkErr := w.client.Conn().GetW(w.Path)
	if zkErr != nil {
		if zkErr == zk.ErrNoNode {
			data, zkErr = p.Get()
			if zkErr != nil {
				return nil, zkErr
			}
			zkErr = w.client.SetRawValue(w.Path, data)
			if zkErr != nil {
				return nil, zkErr
			}
			data, _, wch, zkErr = w.client.Conn().GetW(w.Path)
		}

		if zkErr != nil {
			return nil, zkErr
		}
	}

	if data == nil {
		// ignore nil config
		return wch, nil
	}

	zkErr = p.Set(data)
	if zkErr != nil {
		if zkErr == io.EOF {
			return wch, nil // ignore nil data
		}
		logger.Warnf("failed to parse %s: %v", w.Path, zkErr)
		return wch, nil
	}

	return wch, nil
}
