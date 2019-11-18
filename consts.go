// Copyright 2018-2019 The vogo Authors. All rights reserved.
// author: wongoo
// since: 2018/12/27
//

package zkclient

import (
	"reflect"
	"time"
)

const (
	defaultTimeout = time.Second * 5

	PathSplit = "/"
)

var (
	nilStruct = struct{}{}
	nilValue  = reflect.Value{}
)
