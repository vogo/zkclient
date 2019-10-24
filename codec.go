// Copyright 2018-2019 The vogo Authors. All rights reserved.
// author: wongoo
// since: 2018/12/27
//

package zkclient

// Codec for data
type Codec interface {
	// Encode object to byte, which must be a pointer
	Encode(obj interface{}) ([]byte, error)

	// Decode byte data to object, which must be a pointer
	Decode(data []byte) (interface{}, error)
}
