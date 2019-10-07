// author: wongoo
// since: 2018/12/27
//

package zkclient

import "github.com/vogo/logger"

// SetValue in zookeeper
func (cli *Client) SetValue(path string, obj interface{}, codec Codec) error {
	bytes, err := codec.Encode(obj)
	if err != nil {
		return err
	}
	return cli.SetRawValue(path, bytes)
}

// SetValue in zookeeper
func (cli *Client) SetValuePack(path string, pack *PackValue) error {
	bytes, err := pack.Get()
	if err != nil {
		return err
	}
	return cli.SetRawValue(path, bytes)
}

// SetValue in zookeeper
func (cli *Client) SetRawValue(path string, bytes []byte) error {
	logger.Infof("set zk value: [%s] %s", path, string(bytes))
	err := cli.EnsurePath(path)
	if err != nil {
		return err
	}
	_, err = cli.Conn().Set(path, bytes, -1)
	if err != nil {
		return err
	}
	return nil
}

// SetMapValue in zookeeper
func (cli *Client) SetMapValue(path, key string, obj interface{}, codec Codec) error {
	childPath := path + "/" + key
	bytes, err := codec.Encode(obj)
	if err != nil {
		return err
	}
	return cli.SetRawValue(childPath, bytes)
}
