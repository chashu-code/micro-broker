package tio

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"time"
)

// base on https://github.com/ssdb/gossdb

// SSDBPacket 数据包
type SSDBPacket struct {
	args []interface{}
}

// NewSSDBPacket 构造新的SSDBPacket
func NewSSDBPacket(args ...interface{}) *SSDBPacket {
	p := new(SSDBPacket)
	p.args = args
	return p
}

// Update 更新包参数
func (p *SSDBPacket) Update(args ...interface{}) {
	p.args = args
}

// ToMsg 数据包导出生成Msg
func (p *SSDBPacket) ToMsg() *Msg {
	cmd := p.CMD()
	args := p.StrArgs()
	lenArgs := len(args)

	msg := &Msg{
		Action: cmd,
	}

	switch cmd {
	case CmdReqSend, CmdJobSend:
		if lenArgs < 3 {
			return nil
		}
		msg.Service = args[0]
		msg.Nav = args[1]
		msg.Data = args[2]
	case CmdReqRemote, CmdReqRecv, CmdJobRemote:
		if lenArgs < 4 {
			return nil
		}
		msg.Service = args[0]
		msg.Nav = args[1]
		msg.Data = args[2]
		msg.From = args[3]
	case CmdResSend, CmdResRemote:
		if lenArgs < 3 {
			return nil
		}
		msg.Code = args[0]
		msg.Data = args[1]
		msg.From = args[2]
	case CmdResRecv:
		if lenArgs < 2 {
			return nil
		}
		msg.Code = args[0]
		msg.Data = args[1]
	default:
		return nil
	}
	return msg
}

// CMD 返回数据包的指令
func (p *SSDBPacket) CMD() string {
	if len(p.args) > 0 {
		if s, ok := p.args[0].(string); ok {
			return s
		}
	}
	return ""
}

// Bytes 序列化成 bytes
func (p *SSDBPacket) Bytes() []byte {
	var buf bytes.Buffer
	for _, arg := range p.args {
		var s string
		switch arg := arg.(type) {
		case string:
			s = arg
		case []byte:
			s = string(arg)
		case []string:
			for _, s := range arg {
				buf.WriteString(fmt.Sprintf("%d", len(s)))
				buf.WriteByte('\n')
				buf.WriteString(s)
				buf.WriteByte('\n')
			}
			continue
		case int:
			s = fmt.Sprintf("%d", arg)
		case int64:
			s = fmt.Sprintf("%d", arg)
		case float64:
			s = fmt.Sprintf("%f", arg)
		case bool:
			if arg {
				s = "1"
			} else {
				s = "0"
			}
		case nil:
			s = ""
		default:
			return nil
		}
		buf.WriteString(fmt.Sprintf("%d", len(s)))
		buf.WriteByte('\n')
		buf.WriteString(s)
		buf.WriteByte('\n')
	}
	buf.WriteByte('\n')

	return buf.Bytes()
}

// StrArgs 参数转换成 []string
func (p *SSDBPacket) StrArgs() []string {
	size := len(p.args) - 1
	if size < 1 {
		return nil
	}
	res := make([]string, size)
	var ok bool
	for i, v := range p.args[1:] {
		res[i], ok = v.(string)
		if !ok {
			res[i] = fmt.Sprintf("%v", v)
		}
	}
	return res
}

// SSDBProtocol SSDB协议
type SSDBProtocol struct {
	bufRecv         bytes.Buffer
	timeoutDeadLine time.Duration
}

// ReadPacket 从conn中读取并分析提取出 Packet
func (p *SSDBProtocol) ReadPacket(conn net.Conn) (IPacket, error) {
	var tmp [8192]byte
	for {
		resp := p.parse()
		if resp == nil || len(resp) > 0 {
			return NewSSDBPacket(resp...), nil
		}
		if p.timeoutDeadLine > 0 {
			// p.sock.SetReadDeadline(time.Now().Add(p.durationTimeout))
		}
		n, err := conn.Read(tmp[0:])
		if err != nil {
			return nil, err
		}
		p.bufRecv.Write(tmp[0:n])
	}
}

func (p *SSDBProtocol) parse() []interface{} {
	resp := []interface{}{}
	buf := p.bufRecv.Bytes()
	var idx, offset int
	idx = 0
	offset = 0

	for {
		idx = bytes.IndexByte(buf[offset:], '\n')
		if idx == -1 {
			break
		}
		b := buf[offset : offset+idx]
		offset += idx + 1
		if len(b) == 0 || (len(b) == 1 && b[0] == '\r') {
			if len(resp) == 0 {
				continue
			} else {
				p.bufRecv.Next(offset)
				return resp
			}
		}

		size, err := strconv.Atoi(string(b))
		if err != nil || size < 0 {
			return nil
		}
		if offset+size >= p.bufRecv.Len() {
			break
		}

		v := buf[offset : offset+size]
		resp = append(resp, string(v))
		offset += size + 1
	}

	return []interface{}{}
}
