// Copyright 2018-2019 The vogo Authors. All rights reserved.
// author: wongoo
// since: 2018/12/3
//

package zkclient

import (
	"fmt"
	"net"
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
	ClientOptions
	servers      []string
	conn         *zk.Conn
	done         chan struct{}
	deadWatchers []*Watcher
	dialer       zk.Dialer
}

// NewClient zookeeper client
func NewClient(servers []string, options ...ClientOption) *Client {
	client := new(Client)
	client.servers = servers
	client.done = make(chan struct{})
	client.dialer = func(network, address string, dialTimeout time.Duration) (net.Conn, error) {
		conn, err := net.DialTimeout(network, address, dialTimeout)
		if err != nil && client.alarmTrigger != nil {
			client.alarmTrigger(err)
		}

		return conn, err
	}

	for _, option := range options {
		option(&client.ClientOptions)
	}

	if client.timeout <= 0 {
		client.timeout = defaultTimeout
	}

	_ = client.connect()

	client.startConnMaintainer()

	return client
}

// collectDeadWatchers start a goroutine to maintain the zk connection
func (cli *Client) startConnMaintainer() {
	go func() {
		ticker := time.NewTicker(time.Second * 20)
		for {
			select {
			case <-cli.done:
				return
			case <-ticker.C:
				if !cli.ConnAlive() && !cli.Connecting() {
					_ = cli.Reconnect()
				}

				if cli.ConnAlive() && len(cli.deadWatchers) > 0 {
					for _, watcher := range cli.collectDeadWatchers() {
						watcher.Watch()
					}
				}
			}
		}
	}()
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

// zkLogger logger for zookeeper
type zkLogger struct {
}

// Printf only write log when debug level enabled
func (l *zkLogger) Printf(format string, a ...interface{}) {
	if logger.Level < logger.LevelDebug {
		return
	}

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
	cli.Lock()
	defer cli.Unlock()

	select {
	case <-cli.done:
	default:
		close(cli.done)
	}

	// notify dead watchers
	for _, watcher := range cli.deadWatchers {
		watcher.Close()
	}

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
			if err := cli.EnsurePath(ParentNode(path)); err != nil {
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
	logger.Debugf("zk delete node [%s]", path)

	if err := cli.conn.Delete(path, -1); err != nil && err != zk.ErrNoNode {
		return err
	}

	return nil
}
