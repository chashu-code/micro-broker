package tio

import "net"

// IPacket 包接口
type IPacket interface {
	Bytes() []byte
	ToMsg() *Msg
	Cmds() []string
	Data() []byte
	UpdateCmds(...string)
	UpdateData([]byte)
}

// IProtocol 协议接口
type IProtocol interface {
	ReadPacket(conn net.Conn) (IPacket, error)
	Marshal(interface{}) ([]byte, error)
}
