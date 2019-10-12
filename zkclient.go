// Copyright 2019 The vogo Authors. All rights reserved.
// author: wongoo
// since: 2018/12/3
//

package zkclient

import (
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/vogo/logger"
)

// AlarmTrigger alarm function
type AlarmTrigger func(err error)

// Client for zookeeper
type Client struct {
	sync.Mutex
	servers      []string
	timeout      time.Duration
	conn         *zk.Conn
	close        chan struct{}
	deadWatchers []*Watcher
	dialer       zk.Dialer
	alarmTrigger AlarmTrigger
}

// NewClient zookeeper client
func NewClient(servers []string, timeout time.Duration) (*Client, error) {
	client := new(Client)
	client.servers = servers
	client.timeout = timeout
	client.close = make(chan struct{})
	client.dialer = func(network, address string, dialTimeout time.Duration) (net.Conn, error) {
		conn, err := net.DialTimeout(network, address, dialTimeout)
		if err != nil && client.alarmTrigger != nil {
			client.alarmTrigger(err)
		}

		return conn, err
	}

	err := client.connect()
	if err != nil {
		return nil, err
	}

	go func() {
		ticker := time.NewTicker(time.Second * 20)
		select {
		case <-client.close:
			return
		case <-ticker.C:
			if !client.ConnAlive() && !client.Connecting() {
				_ = client.Reconnect()
			}

			if client.ConnAlive() && len(client.deadWatchers) > 0 {
				for _, watcher := range client.collectDeadWatchers() {
					watcher.Watch()
				}
			}
		}
	}()

	return client, err
}

// SetAlarmTrigger set zookeeper alarm function
func (cli *Client) SetAlarmTrigger(f AlarmTrigger) {
	cli.alarmTrigger = f
}

// collectDeadWatchers return queued watchers, and empty the queue
func (cli *Client) collectDeadWatchers() []*Watcher {
	cli.Lock()
	defer cli.Unlock()

	watchers := cli.deadWatchers
	cli.deadWatchers = []*Watcher{}

	return watchers
}

// AppendDeadWatcher add dead watcher, wait to watch again
func (cli *Client) AppendDeadWatcher(watcher *Watcher) {
	cli.Lock()
	defer cli.Unlock()

	logger.Debugf("zk watcher append to dead queue: %s", watcher.handler.Path())
	watcher.client = cli
	cli.deadWatchers = append(cli.deadWatchers, watcher)
}

type zkLogger struct {
}

func (l *zkLogger) Printf(format string, a ...interface{}) {
	logger.WriteLog("ZOOK", fmt.Sprintf(format, a...))
}

// Close connection
func (cli *Client) connect() error {
	conn, _, err := zk.Connect(cli.servers, cli.timeout, zk.WithLogger(&zkLogger{}), zk.WithDialer(cli.dialer))
	cli.conn = conn

	if err != nil {
		return err
	}

	return nil
}

// Close client, NOT use Client which already calling Close()
func (cli *Client) Close() {
	close(cli.close)

	if cli.ConnAlive() {
		cli.conn.Close()
	}
}

// Reconnect connection
func (cli *Client) Reconnect() error {
	cli.conn.Close()
	return cli.connect()
}

// ConnAlive check
func (cli *Client) ConnAlive() bool {
	return StateAlive(cli.conn.State())
}

// Connecting check
func (cli *Client) Connecting() bool {
	return cli.conn.State() == zk.StateConnecting
}

// Conn for zookeeper
func (cli *Client) Conn() *zk.Conn {
	return cli.conn
}

// EnsurePath check or create target path
func (cli *Client) EnsurePath(path string) error {
	exists, _, err := cli.conn.Exists(path)
	if err != nil {
		return err
	}

	if !exists {
		_, err := cli.conn.Create(path, []byte(""), 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			if err != zk.ErrNoNode {
				return err
			}

			// create parent
			if err := cli.EnsurePath(filepath.Dir(path)); err != nil {
				return err
			}

			// create again
			if _, err := cli.conn.Create(path, []byte(""), 0, zk.WorldACL(zk.PermAll)); err != nil {
				return err
			}
		}
	}

	return nil
}

// Delete path
func (cli *Client) Delete(path string) error {
	if err := cli.conn.Delete(path, -1); err != nil && err != zk.ErrNoNode {
		return err
	}

	return nil
}
