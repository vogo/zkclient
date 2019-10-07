// Copyright 2019 The vogo Authors. All rights reserved.
// author: wongoo
// since: 2018/12/27
//

package zkclient

import (
	"io"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/vogo/logger"
)

// Sync synchronize value of the path to obj
func (cli *Client) Sync(path string, obj interface{}, codec Codec) error {
	valuePack, err := Pack(obj, codec)
	if err != nil {
		return err
	}

	watcher, err := cli.NewWatcher(path, func(client *Client, evt *zk.Event) (<-chan zk.Event, error) {
		data, _, wch, zkErr := client.Conn().GetW(path)
		if zkErr != nil {
			if zkErr == zk.ErrNoNode {
				zkErr = client.SetPackValue(path, valuePack)
				if zkErr != nil {
					return nil, zkErr
				}
				data, _, wch, zkErr = client.Conn().GetW(path)
			}
			if zkErr != nil {
				return nil, zkErr
			}
		}

		if data == nil {
			// ignore nil config
			return wch, nil
		}

		zkErr = valuePack.Set(data)
		if zkErr != nil {
			if zkErr == io.EOF {
				return wch, nil // ignore nil data
			}
			logger.Warnf("failed to parse %s: %v", path, zkErr)
			return wch, nil
		}

		return wch, nil
	})

	if err != nil {
		return err
	}

	watcher.Watch()

	return nil
}
