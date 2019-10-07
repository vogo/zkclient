// author: wongoo
// since: 2018/12/27
//

package zkclient

import (
	"reflect"
)

// Codec for data
type Codec interface {
	Encode(obj interface{}) ([]byte, error)
	Decode(data []byte, typ reflect.Type) (reflect.Value, error)
}
