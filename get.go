// Copyright 2019 The vogo Authors. All rights reserved.
// author: wongoo
// since: 2018/12/27
//

package zkclient

import (
	"reflect"
)

// Get value from zookeeper, the raw value will be decoded by codec
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
