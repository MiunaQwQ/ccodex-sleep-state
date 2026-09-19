//go:build !darwin

package netpath

import (
	"context"
	"errors"
)

func discover(context.Context, string) (Info, error) {
	return Info{}, errors.New("当前平台尚不支持物理网卡模式，请选择系统网络模式")
}
