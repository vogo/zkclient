// Copyright 2019 The vogo Authors. All rights reserved.
// author: wongoo
// since: 2018/12/27
//

package zkclient

import (
	"encoding/json"
	"reflect"
)

// Encode value from zookeeper, the raw value will be decoded by codec
func (cli *Client) Get(path string, codec Codec, typ reflect.Type) (interface{}, error) {
	data, _, err := cli.Conn().Get(path)
	if err != nil {
		return nil, err
	}

	v, err := codec.Decode(data, typ)
	if err != nil {
		return nil, err
	}
	return v.Interface(), nil
}

// GetString get string value from zookeeper
func (cli *Client) GetString(path string) (string, error) {
	data, _, err := cli.Conn().Get(path)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// GetJSON get json value from zookeeper
func (cli *Client) GetJSON(path string, typ reflect.Type) (interface{}, error) {
	data, _, err := cli.Conn().Get(path)
	if err != nil {
		return nil, err
	}

	return jsonCodec.Decode(data, typ)
}

// ParseJSON parse json value from zookeeper into target object
func (cli *Client) ParseJSON(path string, target interface{}) error {
	data, _, err := cli.Conn().Get(path)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, target)
}
