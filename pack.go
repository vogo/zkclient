// author: wongoo
// since: 2018/12/27
//

package zkclient

import (
	"errors"
	"io"
	"reflect"
	"sync"
)

type PackValue struct {
	value reflect.Value
	typ   reflect.Type
	codec Codec
}

func PackString(s *string) *PackValue {
	return &PackValue{
		value: reflect.ValueOf(s),
		typ:   reflect.TypeOf(s),
		codec: stringCodec,
	}
}

func Pack(obj interface{}, codec Codec) (*PackValue, error) {
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
	return &PackValue{
		value: reflect.ValueOf(obj),
		typ:   typ,
		codec: codec,
	}, nil
}

func (p *PackValue) Get() ([]byte, error) {
	return p.codec.Encode(p.value.Interface())
}

func (p *PackValue) Set(data []byte) error {
	v, err := p.codec.Decode(data, p.typ)
	if err != nil {
		return err
	}
	p.value.Elem().Set(v.Elem())
	return nil
}

type PackMap struct {
	value    reflect.Value
	typ      reflect.Type
	codec    Codec
	lock     sync.Mutex
	children map[string]struct{}
}

func MapPack(obj interface{}, codec Codec) (*PackMap, error) {
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
	return &PackMap{
		value:    reflect.ValueOf(obj),
		typ:      valueTyp,
		codec:    codec,
		lock:     sync.Mutex{},
		children: make(map[string]struct{}),
	}, nil
}

func (p *PackMap) Get(key string) ([]byte, error) {
	v := p.value.MapIndex(reflect.ValueOf(key))
	if v.IsNil() {
		return nil, io.EOF
	}
	return p.codec.Encode(v.Interface())
}

func (p *PackMap) Set(key string, data []byte) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	v, err := p.codec.Decode(data, p.typ)
	if err != nil {
		return err
	}
	p.value.SetMapIndex(reflect.ValueOf(key), v)
	return nil
}

func (p *PackMap) Delete(key string) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.value.SetMapIndex(reflect.ValueOf(key), reflect.Value{})
}
