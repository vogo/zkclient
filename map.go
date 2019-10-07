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

// loadMapChild init
func (cli *Client) loadMapChild(packMap *PackMap, path, child string, syncChildChange bool) (<-chan zk.Event, error) {
	var data []byte
	var err error
	var ch <-chan zk.Event

	childPath := path + "/" + child
	logger.Debugf("read path --> %s", childPath)
	if syncChildChange {
		data, _, ch, err = cli.Conn().GetW(childPath)
		if err != nil {
			return nil, err
		}
	} else {
		data, _, err = cli.Conn().Get(childPath)
		if err != nil {
			return nil, err
		}
	}

	err = packMap.Set(child, data)

	if err != nil {
		if err != io.EOF {
			logger.Warnf("failed to parse %s: %v", childPath, err)
		}
		return ch, nil
	}

	return ch, nil
}

// SyncMap config
// syncChildChange : whether sync the change event of the data of children
func (cli *Client) SyncMap(path string, m interface{}, valueCodec Codec, callback EventCallback, syncChildChange bool) error {
	packMap, err := MapPack(m, valueCodec)
	if err != nil {
		return err
	}

	watcher, err := cli.NewWatcher(path, newMapEventHandler(packMap, path, syncChildChange, callback))

	if err != nil {
		return err
	}
	watcher.Watch()
	return nil
}

func newMapEventHandler(packMap *PackMap, path string, syncChildChange bool, callback EventCallback) EventHandler {
	return func(client *Client, evt *zk.Event) (<-chan zk.Event, error) {
		children, _, wch, err := client.Conn().ChildrenW(path)
		if err != nil {
			if err == zk.ErrNoNode {
				_ = client.EnsurePath(path)
				children, _, wch, err = client.Conn().ChildrenW(path)
			}
			if err != nil {
				return nil, err
			}
		}

		newChildren := make(map[string]struct{})
		oldChildren := packMap.children
		for _, child := range children {
			newChildren[child] = nilStruct
			if _, ok := oldChildren[child]; !ok {
				syncMapChild(client, packMap, path, child, syncChildChange)
			}
		}

		for child := range oldChildren {
			if _, ok := newChildren[child]; !ok {
				logger.Infof("zk delete path: %s/%s", path, child)
				packMap.Delete(child)
			}
		}

		packMap.children = newChildren

		if callback != nil {
			if err := callback(); err != nil {
				return nil, err
			}
		}

		return wch, nil
	}
}

func syncMapChild(client *Client, packMap *PackMap, path, child string, syncChildChange bool) {
	if !syncChildChange {
		_, err := client.loadMapChild(packMap, path, child, syncChildChange)
		if err != nil {
			logger.Errorf("load zk map child error: %v", err)
		}
		return
	}

	childPath := path + "/" + child
	childWatcher, err := client.NewWatcher(childPath, func(client *Client, evt *zk.Event) (<-chan zk.Event, error) {
		if evt != nil && evt.Type == zk.EventNodeDeleted {
			return nil, nil // return nil chan to exit watching
		}

		return client.loadMapChild(packMap, path, child, syncChildChange)
	})
	if err != nil {
		logger.Errorf("watch zk map child error: %v", err)
		return
	}

	childWatcher.Watch()
}
