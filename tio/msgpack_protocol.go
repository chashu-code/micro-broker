package tio

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/ugorji/go/codec"
)

var mh codec.MsgpackHandle

// MPPacket MsgPack Packet
type MPPacket struct {
	cmds []string
	data []byte
}

// NewMPPacket 构造新的MPPacket
func NewMPPacket() *MPPacket {
	p := new(MPPacket)
	return p
}

// UpdateCmds 更新命令
func (p *MPPacket) UpdateCmds(cmds ...string) {
	p.cmds = cmds
}

// UpdateData 更新附加数据
func (p *MPPacket) UpdateData(data []byte) {
	p.data = data
}

// ToMsg 数据包导出生成Msg
func (p *MPPacket) ToMsg() *Msg {
	if len(p.cmds) < 2 {
		return nil
	}

	cmd := p.cmds[0]
	args := p.cmds[1:]
	lenArgs := len(args)

	msg := &Msg{
		Action: cmd,
	}

	switch cmd {
	case CmdReqSend, CmdJobSend:
		if lenArgs < 2 {
			return nil
		}
		msg.Service = args[0]
		msg.Nav = args[1]
	case CmdReqRemote, CmdReqRecv, CmdJobRemote:
		if lenArgs < 3 {
			return nil
		}
		msg.Service = args[0]
		msg.Nav = args[1]
		msg.From = args[2]
	case CmdResSend, CmdResRemote:
		if lenArgs < 2 {
			return nil
		}
		msg.Code = args[0]
		msg.From = args[1]
	case CmdResRecv:
		if lenArgs < 1 {
			return nil
		}
		msg.Code = args[0]
	default:
		return nil
	}
	msg.Data = p.data
	return msg
}

// Cmds 返回数据包的指令
func (p *MPPacket) Cmds() []string {
	return p.cmds
}

// Data 返回数据包的附带数据
func (p *MPPacket) Data() []byte {
	return p.data
}

// Bytes 序列化成 bytes
func (p *MPPacket) Bytes() []byte {
	// cmds
	var cmdBytes []byte
	enc := codec.NewEncoderBytes(&cmdBytes, &mh)
	enc.Encode(p.cmds)

	sizeCmd := len(cmdBytes)
	sizeData := len(p.data)

	// result
	resBytes := make([]byte, 8+sizeCmd+sizeData)

	binary.BigEndian.PutUint32(resBytes[0:4], uint32(sizeCmd))
	binary.BigEndian.PutUint32(resBytes[4:8], uint32(sizeData))

	posCmdEnd := 8 + sizeCmd
	if sizeCmd > 0 {
		copy(resBytes[8:posCmdEnd], cmdBytes)
	} else {
		return resBytes
	}

	if sizeData > 0 {
		copy(resBytes[posCmdEnd:], p.data)
	}
	return resBytes
}

// MPProtocol MP协议
type MPProtocol struct {
	timeoutDeadLine time.Duration
}

// ReadPacket 从conn中读取并分析提取出 Packet
func (p *MPProtocol) ReadPacket(conn net.Conn) (IPacket, error) {
	sizeBytes := make([]byte, 8)

	// fmt.Println("read size bytes")
	if _, err := io.ReadFull(conn, sizeBytes); err != nil {
		return nil, fmt.Errorf("packet read size fail: %v", err)
	}

	sizeCmd := binary.BigEndian.Uint32(sizeBytes[0:4])
	sizeData := binary.BigEndian.Uint32(sizeBytes[4:8])

	if sizeCmd < 1 {
		return nil, fmt.Errorf("packet cmd size < 1")
	}

	cmdBytes := make([]byte, sizeCmd)
	dataBytes := make([]byte, sizeData)

	// fmt.Printf("read cmdBytes: %v \n", sizeCmd)
	if _, err := io.ReadFull(conn, cmdBytes); err != nil {
		return nil, fmt.Errorf("packet read cmds fail: %v", err)
	}

	if sizeData > 0 {
		// fmt.Printf("read dataBytes: %v \n", sizeData)
		if _, err := io.ReadFull(conn, dataBytes); err != nil {
			return nil, fmt.Errorf("packet read data fail: %v", err)
		}
	}

	packet := NewMPPacket()

	dec := codec.NewDecoderBytes(cmdBytes, &mh)
	dec.Decode(&packet.cmds)
	packet.UpdateData(dataBytes)

	return packet, nil
}

// Marshal 序列化为 []byte
func (p *MPProtocol) Marshal(v interface{}) ([]byte, error) {
	var dataBytes []byte
	enc := codec.NewEncoderBytes(&dataBytes, &mh)
	err := enc.Encode(v)
	return dataBytes, err
}
