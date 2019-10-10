// Copyright 2019 The vogo Authors. All rights reserved.
// author: wongoo

package main

import (
	"time"

	"github.com/vogo/logger"
	"github.com/vogo/zkclient"
)

func main() {
	client, err := zkclient.NewClient([]string{"127.0.0.1:2181"}, time.Second*5)
	if err != nil {
		logger.Fatalf("failed to connect zookeeper: %v", err)
	}

	if err := syncString(client); err != nil {
		logger.Fatal(err)
	}
	if err := syncJson(client); err != nil {
		logger.Fatal(err)
	}
	if err := syncMap(client); err != nil {
		logger.Fatal(err)
	}
}

func syncString(client *zkclient.Client) error {
	var test string
	logger.Infof("before set: %s", test)

	if _, err := client.SyncWatchString("/test", &test, nil); err != nil {
		return err
	}

	time.Sleep(time.Second)
	logger.Infof("string after sync: %s", test)

	if err := client.SetString("/test", "hello world"); err != nil {
		return err
	}

	time.Sleep(time.Second)
	logger.Infof("string after set 1: %s", test)

	if err := client.SetString("/test", "hello"); err != nil {
		return err
	}
	time.Sleep(time.Second)
	logger.Infof("string after set 2: %s", test)

	if err := client.SetString("/test", ""); err != nil {
		return err
	}
	time.Sleep(time.Second)
	logger.Infof("string after set 3: %s", test)

	return nil
}

type user struct {
	Name string
	Sex  int
}

func syncJson(client *zkclient.Client) error {
	u := &user{}

	path := "/test/user"
	if _, err := client.SyncWatchJSON(path, u, nil); err != nil {
		return err
	}
	time.Sleep(time.Second)
	logger.Infof("user after sync: %v", u)

	if err := client.SetRawValue(path, []byte(`{"name":"wongoo", "sex":1}`)); err != nil {
		return err
	}
	time.Sleep(time.Second)
	logger.Infof("user after set: %v", u)

	if err := client.SetRawValue(path, []byte(`{"name":"jack", "sex":1}`)); err != nil {
		return err
	}
	time.Sleep(time.Second)
	logger.Infof("user after set: %v", u)

	if err := client.SetRawValue(path, []byte("{}")); err != nil {
		return err
	}
	time.Sleep(time.Second)
	logger.Infof("user after clean: %v", u)

	return nil
}

func syncMap(client *zkclient.Client) error {
	path := "/test/users"
	users := make(map[string]*user)
	if err := client.SyncWatchJSONMap(path, users, true, nil); err != nil {
		return err
	}
	time.Sleep(time.Second)
	logger.Infof("users after sync: %v", users)

	if err := client.SetMapJSONValue(path, "u1", &user{Name: "wongoo", Sex: 1}); err != nil {
		return err
	}
	time.Sleep(time.Second)
	logger.Infof("users after set u1: %v", users)

	if err := client.SetMapJSONValue(path, "u1", &user{Name: "yang", Sex: 1}); err != nil {
		return err
	}
	time.Sleep(time.Second)
	logger.Infof("users after change u1: %v", users)

	if err := client.SetMapJSONValue(path, "u2", &user{Name: "jack", Sex: 0}); err != nil {
		return err
	}
	time.Sleep(time.Second)
	logger.Infof("users after set u2: %v", users)

	if err := client.Delete(path + "/u1"); err != nil {
		return err
	}
	time.Sleep(time.Second)
	logger.Infof("users after delete u1: %v", users)

	if err := client.Delete(path + "/u2"); err != nil {
		return err
	}
	time.Sleep(time.Second)
	logger.Infof("users after delete u2: %v", users)

	return nil
}
