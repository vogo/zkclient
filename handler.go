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
	path     string
	value    reflect.Value
	typ      reflect.Type
	codec    Codec
	listener ValueListener
}

func StringValueHandler(path string, s *string, listener ValueListener) *ValueHandler {
	return &ValueHandler{
		path:     path,
		value:    reflect.ValueOf(s),
		typ:      reflect.TypeOf(s),
		codec:    stringCodec,
		listener: listener,
	}
}

func NewValueHandler(path string, obj interface{}, codec Codec, listener ValueListener) (*ValueHandler, error) {
	if path == "" {
		return nil, errors.New("path required")
	}

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
		path:     path,
		value:    reflect.ValueOf(obj),
		typ:      typ,
		codec:    codec,
		listener: listener,
	}, nil
}

func (h *ValueHandler) Encode() ([]byte, error) {
	return h.codec.Encode(h.value.Interface())
}

func (h *ValueHandler) Decode(data []byte) error {
	v, err := h.codec.Decode(data, h.typ)
	if err != nil {
		return err
	}

	h.value.Elem().Set(v.Elem())

	if h.listener != nil {
		go func() {
			h.listener(h.path, h.value.Interface())
		}()
	}

	return nil
}

// SetTo set value in zookeeper
func (h *ValueHandler) SetTo(cli *Client, path string) error {
	bytes, err := h.Encode()
	if err != nil {
		return err
	}

	return cli.SetRawValue(path, bytes)
}

func (h *ValueHandler) Path() string {
	return h.path
}

func (h *ValueHandler) Handle(w *Watcher, evt *zk.Event) (<-chan zk.Event, error) {
	data, _, wch, err := w.client.Conn().GetW(h.path)
	if err != nil {
		if err == zk.ErrNoNode {
			data, err = h.Encode()
			if err != nil {
				return nil, err
			}

			if setErr := w.client.SetRawValue(h.path, data); setErr != nil {
				return nil, setErr
			}

			data, _, wch, err = w.client.Conn().GetW(h.path)
		}

		if err != nil {
			return nil, err
		}
	}

	if data == nil {
		// ignore nil config
		return wch, nil
	}

	if err := h.Decode(data); err != nil {
		if err == io.EOF {
			return wch, nil // ignore nil data
		}

		logger.Warnf("zk failed to parse %s: %v", h.path, err)

		return wch, nil
	}

	return wch, nil
}
