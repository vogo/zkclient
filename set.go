// Copyright 2019 The vogo Authors. All rights reserved.
// author: wongoo
// since: 2018/12/27
//

package zkclient

import "github.com/vogo/logger"

// SetValue set value in zookeeper
func (cli *Client) SetValue(path string, obj interface{}, codec Codec) error {
	bytes, err := codec.Encode(obj)
	if err != nil {
		return err
	}

	return cli.SetRawValue(path, bytes)
}

// SetRawValue set raw value in zookeeper
func (cli *Client) SetRawValue(path string, bytes []byte) error {
	logger.Debugf("set zk value: [%s] %s", path, string(bytes))

	if err := cli.EnsurePath(path); err != nil {
		return err
	}

	if _, err := cli.Conn().Set(path, bytes, -1); err != nil {
		return err
	}

	return nil
}

// SetString in zookeeper
func (cli *Client) SetString(path, s string) error {
	return cli.SetRawValue(path, []byte(s))
}

// SetJSON in zookeeper
func (cli *Client) SetJSON(path string, obj interface{}) error {
	return cli.SetValue(path, obj, jsonCodec)
}

// SetMapValue set map value in zookeeper
func (cli *Client) SetMapValue(path, key string, obj interface{}, codec Codec) error {
	childPath := path + "/" + key
	bytes, err := codec.Encode(obj)

	if err != nil {
		return err
	}

	return cli.SetRawValue(childPath, bytes)
}

// SetMapStringValue in zookeeper
func (cli *Client) SetMapStringValue(path, key, s string) error {
	return cli.SetRawValue(path+"/"+key, []byte(s))
}

// SetMapJSONValue in zookeeper
func (cli *Client) SetMapJSONValue(path, key string, obj interface{}) error {
	return cli.SetMapValue(path, key, obj, jsonCodec)
}
