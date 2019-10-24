// Copyright 2018-2019 The vogo Authors. All rights reserved.
// author: wongoo
// since: 2018/12/27
//

package zkclient

// Sync synchronize value of the path to obj
func (cli *Client) Sync(path string, obj interface{}, codec Codec) (*Watcher, error) {
	return cli.SyncWatch(path, obj, codec, nil)
}

// SyncWatch synchronize value of the path to obj, and trigger listener when value change
func (cli *Client) SyncWatch(path string, obj interface{}, codec Codec, listener ValueListener) (*Watcher, error) {
	handler, err := cli.newValueHandler(path, obj, codec, false, listener)
	if err != nil {
		return nil, err
	}

	return cli.createWatcher(handler)
}

// Watch synchronize value of the path to obj, and trigger listener when value change
func (cli *Client) Watch(path string, obj interface{}, codec Codec, listener ValueListener) (*Watcher, error) {
	handler, err := cli.newValueHandler(path, obj, codec, true, listener)
	if err != nil {
		return nil, err
	}

	return cli.createWatcher(handler)
}

func (cli *Client) createWatcher(handler EventHandler) (*Watcher, error) {
	watcher, err := cli.NewWatcher(handler)

	if err != nil {
		return nil, err
	}

	watcher.Watch()

	return watcher, nil
}

// SyncWatchJSON synchronize json value of the path to obj, and trigger listener when value change
func (cli *Client) SyncWatchJSON(path string, obj interface{}, listener ValueListener) (*Watcher, error) {
	return cli.SyncWatch(path, obj, &JSONCodec{}, listener)
}

// SyncWatchJSON synchronize string value of the path to obj, and trigger listener when value change
func (cli *Client) SyncWatchString(path string, s *string, listener ValueListener) (*Watcher, error) {
	return cli.SyncWatch(path, s, stringCodec, listener)
}

// WatchJSON watch json value of the path to obj, and trigger listener when value change
func (cli *Client) WatchJSON(path string, obj interface{}, listener ValueListener) (*Watcher, error) {
	return cli.Watch(path, obj, &JSONCodec{}, listener)
}

// WatchJSON watch string value of the path to obj, and trigger listener when value change
func (cli *Client) WatchString(path string, s *string, listener ValueListener) (*Watcher, error) {
	return cli.Watch(path, s, stringCodec, listener)
}

// SyncMap synchronize sub-path value into a map
func (cli *Client) SyncMap(path string, m interface{}, valueCodec Codec, syncChild bool) (*Watcher, error) {
	return cli.SyncWatchMap(path, m, valueCodec, syncChild, nil)
}

// SyncWatchMap synchronize sub-path value into a map, and trigger listener when child value change
func (cli *Client) SyncWatchMap(path string, m interface{}, valueCodec Codec, syncChild bool, listener ChildListener) (*Watcher, error) {
	handler, err := cli.newMapHandler(path, m, syncChild, valueCodec, false, listener)
	if err != nil {
		return nil, err
	}

	return cli.createWatcher(handler)
}

// SyncWatchMap synchronize sub-path value into a map, and trigger listener when child value change
func (cli *Client) WatchMap(path string, m interface{}, valueCodec Codec, syncChild bool, listener ChildListener) (*Watcher, error) {
	handler, err := cli.newMapHandler(path, m, syncChild, valueCodec, true, listener)
	if err != nil {
		return nil, err
	}

	return cli.createWatcher(handler)
}

// SyncWatchJSONMap synchronize sub-path json value into a map, and trigger listener when child value change
func (cli *Client) SyncWatchJSONMap(path string, m interface{}, syncChild bool, listener ChildListener) (*Watcher, error) {
	return cli.SyncWatchMap(path, m, &JSONCodec{}, syncChild, listener)
}

// SyncWatchStringMap synchronize sub-path string value into a map, and trigger listener when child value change
func (cli *Client) SyncWatchStringMap(path string, m map[string]string, syncChild bool, listener ChildListener) (*Watcher, error) {
	return cli.SyncWatchMap(path, m, stringCodec, syncChild, listener)
}

// WatchJSONMap watch sub-path json value into a map, and trigger listener when child value change
func (cli *Client) WatchJSONMap(path string, m interface{}, syncChild bool, listener ChildListener) (*Watcher, error) {
	return cli.WatchMap(path, m, &JSONCodec{}, syncChild, listener)
}

// WatchStringMap watch sub-path string value into a map, and trigger listener when child value change
func (cli *Client) WatchStringMap(path string, m map[string]string, syncChild bool, listener ChildListener) (*Watcher, error) {
	return cli.WatchMap(path, m, stringCodec, syncChild, listener)
}
