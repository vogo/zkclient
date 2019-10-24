// Copyright 2018-2019 The vogo Authors. All rights reserved.
// author: wongoo
// since: 2018/12/27
//

package zkclient

import (
	"github.com/samuel/go-zookeeper/zk"
	"github.com/vogo/logger"
)

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
	logger.Debugf("zk set node [%s]", path)

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
	return cli.SetValue(path, obj, jsonEncodeCodec)
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
	return cli.SetMapValue(path, key, obj, jsonEncodeCodec)
}

// CreateTempRawValue create temp raw value in zookeeper
func (cli *Client) CreateTempRawValue(path string, bytes []byte) error {
	_, err := cli.Conn().Create(path, bytes, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	return err
}

// SetTempRawValue set temp raw value in zookeeper
func (cli *Client) SetTempRawValue(path string, bytes []byte) error {
	err := cli.CreateTempRawValue(path, bytes)
	if err == nil {
		return nil
	}

	if err != zk.ErrNodeExists {
		return err
	}

	_, err = cli.Conn().Set(path, bytes, -1)

	return err
}

// SetTempString in zookeeper
func (cli *Client) SetTempString(path, s string) error {
	return cli.SetTempRawValue(path, []byte(s))
}

// SetTempValue set temp value in zookeeper
func (cli *Client) SetTempValue(path string, obj interface{}, codec Codec) error {
	bytes, err := codec.Encode(obj)
	if err != nil {
		return err
	}

	return cli.SetTempRawValue(path, bytes)
}

// SetTempJSON in zookeeper
func (cli *Client) SetTempJSON(path string, obj interface{}) error {
	return cli.SetTempValue(path, obj, jsonEncodeCodec)
}
