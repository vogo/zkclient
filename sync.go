// Copyright 2019 The vogo Authors. All rights reserved.
// author: wongoo
// since: 2018/12/27
//

package zkclient

// Sync synchronize value of the path to obj
func (cli *Client) Sync(path string, obj interface{}, codec Codec) (*Watcher, error) {
	return cli.SyncWatch(path, obj, codec, nil)
}

// SyncWatch synchronize value of the path to obj, and trigger listener when value change
func (cli *Client) SyncWatch(path string, obj interface{}, codec Codec, listener WatchListener) (*Watcher, error) {
	handler, err := NewValueHandler(obj, codec, nil)
	if err != nil {
		return nil, err
	}

	watcher, err := cli.NewWatcher(path, handler)

	if err != nil {
		return nil, err
	}

	watcher.Watch()

	return watcher, nil
}

// SyncWatchJSON synchronize json value of the path to obj, and trigger listener when value change
func (cli *Client) SyncWatchJSON(path string, obj interface{}, listener WatchListener) (*Watcher, error) {
	return cli.SyncWatch(path, obj, jsonCodec, listener)
}

// SyncWatchJSON synchronize string value of the path to obj, and trigger listener when value change
func (cli *Client) SyncWatchString(path string, s *string, listener WatchListener) (*Watcher, error) {
	return cli.SyncWatch(path, s, stringCodec, listener)
}

// SyncMap synchronize sub-path value into a map
func (cli *Client) SyncMap(path string, m interface{}, valueCodec Codec, syncChild bool) error {
	return cli.SyncWatchMap(path, m, valueCodec, syncChild, nil)
}

// SyncWatchMap synchronize sub-path value into a map, and trigger listener when child value change
func (cli *Client) SyncWatchMap(path string, m interface{}, valueCodec Codec, syncChild bool, listener ChildListener) error {
	mapHandler, err := NewMapHandler(m, syncChild, valueCodec, listener)
	if err != nil {
		return err
	}

	watcher, err := cli.NewWatcher(path, mapHandler)

	if err != nil {
		return err
	}
	watcher.Watch()
	return nil
}

// SyncWatchMapJSON synchronize sub-path json value into a map, and trigger listener when child value change
func (cli *Client) SyncWatchMapJSON(path string, m interface{}, syncChild bool, listener ChildListener) error {
	return cli.SyncWatchMap(path, m, jsonCodec, syncChild, listener)
}

// SyncWatchMapJSON synchronize sub-path string value into a map, and trigger listener when child value change
func (cli *Client) SyncWatchMapString(path string, m map[string]string, syncChild bool, listener ChildListener) error {
	return cli.SyncWatchMap(path, m, stringCodec, syncChild, listener)
}
