# zkclient - A golang zookeeper client

[![GoDoc](https://godoc.org/github.com/vogo/zkclient?status.svg)](https://godoc.org/github.com/vogo/zkclient)


## Features

- auto reconnect
- set/get/delete value
- support string/json codec, and you can implement your own, see [codec.go](codec.go)
- real-time synchronize data from zookeeper to memory, see [demo](examples/syncdemo.go)


## Reference

- github.com/samuel/go-zookeeper