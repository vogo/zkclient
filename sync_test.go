// Copyright 2018-2019 The vogo Authors. All rights reserved.
// author: wongoo
// since: 2019/10/12
//

package zkclient

import (
	"testing"
	"time"

	zk2 "github.com/samuel/go-zookeeper/zk"

	"github.com/vogo/logger"

	"github.com/stretchr/testify/assert"
)

func TestClient_Sync(t *testing.T) {
	if !isLocalZKAlive(t) {
		return
	}

	path := "/test/s"

	var test string
	w, err := testClient.SyncWatchString(path, &test, nil)
	assert.Nil(t, err)

	time.Sleep(time.Second)

	err = testClient.SetString(path, "hello world")
	assert.Nil(t, err)

	time.Sleep(time.Second)

	assert.Equal(t, "hello world", test)

	err = testClient.SetString(path, "hello")
	assert.Nil(t, err)

	time.Sleep(time.Second)

	assert.Equal(t, "hello", test)

	err = testClient.SetString(path, "")
	assert.Nil(t, err)

	time.Sleep(time.Second)

	assert.Equal(t, "", test)

	err = testClient.Delete(path)
	assert.Nil(t, err)

	time.Sleep(time.Second)

	w.Close()
}

type user struct {
	Name string
	Sex  int
}

func TestClient_SyncJSON(t *testing.T) {
	if !isLocalZKAlive(t) {
		return
	}

	u := &user{}

	path := "/test/user"
	w, err := testClient.SyncWatchJSON(path, u, nil)
	assert.Nil(t, err)

	time.Sleep(time.Second)

	err = testClient.SetRawValue(path, []byte(`{"name":"wongoo", "sex":1}`))
	assert.Nil(t, err)

	time.Sleep(time.Second)

	assert.Equal(t, "wongoo", u.Name)
	assert.Equal(t, 1, u.Sex)

	err = testClient.SetRawValue(path, []byte(`{"name":"jack", "sex":0}`))
	assert.Nil(t, err)

	time.Sleep(time.Second)

	assert.Equal(t, "jack", u.Name)
	assert.Equal(t, 0, u.Sex)

	err = testClient.SetRawValue(path, []byte("{}"))
	assert.Nil(t, err)

	time.Sleep(time.Second)

	assert.Equal(t, "", u.Name)
	assert.Equal(t, 0, u.Sex)

	err = testClient.Delete(path)
	assert.Nil(t, err)

	time.Sleep(time.Second)

	w.Close()
}

type mListener struct {
}

func (l *mListener) Update(path, child string, stat *zk2.Stat, obj interface{}) {
	logger.Infof("----- %s/%s: %v", path, child, obj)
}
func (l *mListener) Delete(path, child string) {
	logger.Infof("----- %s/%s delete", path, child)
}

func TestClient_SyncJSONMap(t *testing.T) {
	if !isLocalZKAlive(t) {
		return
	}

	path := "/test/users"
	users := make(map[string]*user)
	w, err := testClient.SyncWatchJSONMap(path, users, true, &mListener{})
	assert.Nil(t, err)

	time.Sleep(time.Second)

	err = testClient.SetMapJSONValue(path, "u1", &user{Name: "wongoo", Sex: 1})
	assert.Nil(t, err)

	time.Sleep(time.Second)

	assert.Equal(t, 1, len(users))
	assert.Equal(t, "wongoo", users["u1"].Name)
	assert.Equal(t, 1, users["u1"].Sex)

	err = testClient.SetMapJSONValue(path, "u1", &user{Name: "yang", Sex: 0})
	assert.Nil(t, err)

	time.Sleep(time.Second)

	assert.Equal(t, 1, len(users))
	assert.Equal(t, "yang", users["u1"].Name)
	assert.Equal(t, 0, users["u1"].Sex)

	err = testClient.SetMapJSONValue(path, "u2", &user{Name: "jack", Sex: 0})
	assert.Nil(t, err)

	time.Sleep(time.Second)

	assert.Equal(t, 2, len(users))
	assert.Equal(t, "jack", users["u2"].Name)
	assert.Equal(t, 0, users["u2"].Sex)

	err = testClient.Delete(path + "/u1")
	assert.Nil(t, err)

	time.Sleep(time.Second)

	assert.Equal(t, 1, len(users))

	err = testClient.Delete(path + "/u2")
	assert.Nil(t, err)

	time.Sleep(time.Second)

	assert.Equal(t, 0, len(users))

	w.Close()
}
