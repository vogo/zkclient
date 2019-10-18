// Copyright 2018-2019 The vogo Authors. All rights reserved.
// author: wongoo

package zkclient

import "time"

type ClientOption func(*ClientOptions)

type ClientOptions struct {
	listenAsync  bool
	timeout      time.Duration
	alarmTrigger AlarmTrigger
}

func WithListenAsync(async bool) ClientOption {
	return func(o *ClientOptions) {
		o.listenAsync = async
	}
}

func WithTimeout(timeout time.Duration) ClientOption {
	return func(o *ClientOptions) {
		o.timeout = timeout
	}
}

func WithAlarmTrigger(trigger AlarmTrigger) ClientOption {
	return func(o *ClientOptions) {
		o.alarmTrigger = trigger
	}
}
