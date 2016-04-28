package tio

import "net"

// IPacket 包接口
type IPacket interface {
	Bytes() []byte
	ToMsg() *Msg
	CMD() string
	StrArgs() []string
	Update(...interface{})
}

// IProtocol 协议接口
type IProtocol interface {
	ReadPacket(conn net.Conn) (IPacket, error)
}
