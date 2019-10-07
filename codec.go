// author: wongoo
// since: 2018/12/27
//

package zkclient

import (
	"reflect"
)

// ZKCodec for data
type ZKCodec interface {
	Encode(obj interface{}) ([]byte, error)
	Decode(data []byte, typ reflect.Type) (reflect.Value, error)
}
