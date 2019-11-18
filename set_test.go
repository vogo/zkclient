// Copyright 2018-2019 The vogo Authors. All rights reserved.
// author: wongoo
// since: 2019/10/12
//

package zkclient

import (
	"testing"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
)

func TestClient_SetTempRawValue(t *testing.T) {
	if !isLocalZKAlive(t) {
		return
	}

	path := "/temp_raw_value"

	c := connectLocalZK(t)

	err := c.SetTempString(path, "hello")
	assert.Nil(t, err)

	data, err := c.GetString(path)
	assert.Nil(t, err)
	assert.Equal(t, "hello", data)

	c.Close()
	c = connectLocalZK(t)
	_, err = c.GetString(path)
	assert.NotNil(t, err)
	assert.Equal(t, zk.ErrNoNode, err)
}

func TestClient_SetMapStringValue(t *testing.T) {
	if !isLocalZKAlive(t) {
		return
	}

	path := "/test/set_map_string"

	err := testClient.SetMapStringValue(path, "c1", "hello")
	assert.Nil(t, err)

	childPath := PathJoin(path, "c1")
	data, err := testClient.GetString(childPath)
	assert.Nil(t, err)
	assert.Equal(t, "hello", data)

	err = testClient.Delete(childPath)
	assert.Nil(t, err)
}
