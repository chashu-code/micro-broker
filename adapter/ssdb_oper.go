package adapter

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"time"
)

// SSDBOper SSDB协议操作者
type SSDBOper struct {
	sock            net.Conn
	msTimeout       int
	durationTimeout time.Duration
	bufRecv         bytes.Buffer
}

// NewSSDBOper 构造新的SSDb协议操作者
func NewSSDBOper(sock net.Conn, msTimeout int) *SSDBOper {
	c := &SSDBOper{
		sock:            sock,
		msTimeout:       msTimeout,
		durationTimeout: time.Duration(msTimeout) * time.Millisecond,
	}
	return c
}

// Cmd 发送指令，并接受应答
func (c *SSDBOper) Cmd(args ...interface{}) ([]string, error) {
	err := c.send(args)
	if err != nil {
		return nil, err
	}
	resp, err := c.recv()
	return resp, err
}

// Send 发送指令
func (c *SSDBOper) Send(args ...interface{}) error {
	return c.send(args)
}

func (c *SSDBOper) send(args []interface{}) error {
	var buf bytes.Buffer
	for _, arg := range args {
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
			return fmt.Errorf("bad arguments")
		}
		buf.WriteString(fmt.Sprintf("%d", len(s)))
		buf.WriteByte('\n')
		buf.WriteString(s)
		buf.WriteByte('\n')
	}
	buf.WriteByte('\n')
	if c.msTimeout > 0 {
		c.sock.SetWriteDeadline(time.Now().Add(c.durationTimeout))
	}
	_, err := c.sock.Write(buf.Bytes())
	return err
}

// Recv 接受应答
func (c *SSDBOper) Recv() ([]string, error) {
	return c.recv()
}

func (c *SSDBOper) recv() ([]string, error) {
	var tmp [8192]byte
	for {
		resp := c.parse()
		if resp == nil || len(resp) > 0 {
			return resp, nil
		}
		if c.msTimeout > 0 {
			c.sock.SetReadDeadline(time.Now().Add(c.durationTimeout))
		}
		n, err := c.sock.Read(tmp[0:])
		if err != nil {
			return nil, err
		}
		c.bufRecv.Write(tmp[0:n])
	}
}

func (c *SSDBOper) parse() []string {
	resp := []string{}
	buf := c.bufRecv.Bytes()
	var idx, offset int
	idx = 0
	offset = 0

	for {
		idx = bytes.IndexByte(buf[offset:], '\n')
		if idx == -1 {
			break
		}
		p := buf[offset : offset+idx]
		offset += idx + 1
		if len(p) == 0 || (len(p) == 1 && p[0] == '\r') {
			if len(resp) == 0 {
				continue
			} else {
				c.bufRecv.Next(offset)
				return resp
			}
		}

		size, err := strconv.Atoi(string(p))
		if err != nil || size < 0 {
			return nil
		}
		if offset+size >= c.bufRecv.Len() {
			break
		}

		v := buf[offset : offset+size]
		resp = append(resp, string(v))
		offset += size + 1
	}

	return []string{}
}

// Close The SSDBOper Connection
func (c *SSDBOper) Close() error {
	return c.sock.Close()
}
