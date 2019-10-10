// Copyright 2019 The vogo Authors. All rights reserved.
// author: wongoo
// since: 2019/10/07
//

package zkclient

import "reflect"

type StringCodec struct {
}

func (c *StringCodec) Encode(obj interface{}) ([]byte, error) {
	if s, ok := obj.(string); ok {
		return []byte(s), nil
	}
	return nil, errInvalidValue
}

func (c *StringCodec) Decode(data []byte, typ reflect.Type) (reflect.Value, error) {
	s := string(data)
	return reflect.ValueOf(&s), nil
}

var (
	stringCodec = &StringCodec{}
)

// SetString in zookeeper
func (cli *Client) SetString(path, s string) error {
	return cli.SetRawValue(path, []byte(s))
}

// SetStringMapValue in zookeeper
func (cli *Client) SetStringMapValue(path, key, s string) error {
	return cli.SetRawValue(path+"/"+key, []byte(s))
}
