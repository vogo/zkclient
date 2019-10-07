// Copyright 2019 The vogo Authors. All rights reserved.
// author: wongoo
// since: 2018/12/27
//

package zkclient

import (
	"encoding/json"
	"io"
	"reflect"
)

// JSONCodec struct
type JSONCodec struct{}

// Encode zk json encode
func (c *JSONCodec) Encode(obj interface{}) ([]byte, error) {
	return json.Marshal(obj)
}

// Decode zk json decode
func (c *JSONCodec) Decode(data []byte, typ reflect.Type) (reflect.Value, error) {
	value := reflect.New(typ)
	if len(data) == 0 {
		return value.Elem(), io.EOF
	}

	obj := value.Interface()

	if err := json.Unmarshal(data, obj); err != nil {
		return value, err
	}

	return value.Elem(), nil
}

var jsonCodec = &JSONCodec{}

// SetJSON in zookeeper
func (cli *Client) SetJSON(path string, obj interface{}) error {
	return cli.SetValue(path, obj, jsonCodec)
}

// SyncJSON config
func (cli *Client) SyncJSON(path string, obj interface{}) error {
	return cli.Sync(path, obj, jsonCodec)
}

// SetJSONMapValue in zookeeper
func (cli *Client) SetJSONMapValue(path, key string, obj interface{}) error {
	return cli.SetMapValue(path, key, obj, jsonCodec)
}

// SyncJSONMap config
func (cli *Client) SyncJSONMap(path string, obj interface{}) error {
	return cli.SyncMap(path, obj, jsonCodec, nil, true)
}
