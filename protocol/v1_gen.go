package protocol

// NOTE: THIS FILE WAS PRODUCED BY THE
// MSGP CODE GENERATION TOOL (github.com/tinylib/msgp)
// DO NOT EDIT

import "github.com/tinylib/msgp/msgp"

// DecodeMsg implements msgp.Decodable
func (z *V1Protocol) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zxvk uint32
	zxvk, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zxvk > 0 {
		zxvk--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "act":
			z.Action, err = dc.ReadString()
			if err != nil {
				return
			}
		case "bid":
			z.BID, err = dc.ReadString()
			if err != nil {
				return
			}
		case "rid":
			z.RID, err = dc.ReadString()
			if err != nil {
				return
			}
		case "tid":
			z.TID, err = dc.ReadString()
			if err != nil {
				return
			}
		case "topic":
			z.Topic, err = dc.ReadString()
			if err != nil {
				return
			}
		case "chan":
			z.Channel, err = dc.ReadString()
			if err != nil {
				return
			}
		case "nav":
			z.Nav, err = dc.ReadString()
			if err != nil {
				return
			}
		case "st":
			z.SendTime, err = dc.ReadUint()
			if err != nil {
				return
			}
		case "dl":
			z.DeadLine, err = dc.ReadUint()
			if err != nil {
				return
			}
		case "data":
			z.Data, err = dc.ReadIntf()
			if err != nil {
				return
			}
		case "code":
			z.Code, err = dc.ReadString()
			if err != nil {
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *V1Protocol) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 11
	// write "act"
	err = en.Append(0x8b, 0xa3, 0x61, 0x63, 0x74)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Action)
	if err != nil {
		return
	}
	// write "bid"
	err = en.Append(0xa3, 0x62, 0x69, 0x64)
	if err != nil {
		return err
	}
	err = en.WriteString(z.BID)
	if err != nil {
		return
	}
	// write "rid"
	err = en.Append(0xa3, 0x72, 0x69, 0x64)
	if err != nil {
		return err
	}
	err = en.WriteString(z.RID)
	if err != nil {
		return
	}
	// write "tid"
	err = en.Append(0xa3, 0x74, 0x69, 0x64)
	if err != nil {
		return err
	}
	err = en.WriteString(z.TID)
	if err != nil {
		return
	}
	// write "topic"
	err = en.Append(0xa5, 0x74, 0x6f, 0x70, 0x69, 0x63)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Topic)
	if err != nil {
		return
	}
	// write "chan"
	err = en.Append(0xa4, 0x63, 0x68, 0x61, 0x6e)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Channel)
	if err != nil {
		return
	}
	// write "nav"
	err = en.Append(0xa3, 0x6e, 0x61, 0x76)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Nav)
	if err != nil {
		return
	}
	// write "st"
	err = en.Append(0xa2, 0x73, 0x74)
	if err != nil {
		return err
	}
	err = en.WriteUint(z.SendTime)
	if err != nil {
		return
	}
	// write "dl"
	err = en.Append(0xa2, 0x64, 0x6c)
	if err != nil {
		return err
	}
	err = en.WriteUint(z.DeadLine)
	if err != nil {
		return
	}
	// write "data"
	err = en.Append(0xa4, 0x64, 0x61, 0x74, 0x61)
	if err != nil {
		return err
	}
	err = en.WriteIntf(z.Data)
	if err != nil {
		return
	}
	// write "code"
	err = en.Append(0xa4, 0x63, 0x6f, 0x64, 0x65)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Code)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *V1Protocol) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 11
	// string "act"
	o = append(o, 0x8b, 0xa3, 0x61, 0x63, 0x74)
	o = msgp.AppendString(o, z.Action)
	// string "bid"
	o = append(o, 0xa3, 0x62, 0x69, 0x64)
	o = msgp.AppendString(o, z.BID)
	// string "rid"
	o = append(o, 0xa3, 0x72, 0x69, 0x64)
	o = msgp.AppendString(o, z.RID)
	// string "tid"
	o = append(o, 0xa3, 0x74, 0x69, 0x64)
	o = msgp.AppendString(o, z.TID)
	// string "topic"
	o = append(o, 0xa5, 0x74, 0x6f, 0x70, 0x69, 0x63)
	o = msgp.AppendString(o, z.Topic)
	// string "chan"
	o = append(o, 0xa4, 0x63, 0x68, 0x61, 0x6e)
	o = msgp.AppendString(o, z.Channel)
	// string "nav"
	o = append(o, 0xa3, 0x6e, 0x61, 0x76)
	o = msgp.AppendString(o, z.Nav)
	// string "st"
	o = append(o, 0xa2, 0x73, 0x74)
	o = msgp.AppendUint(o, z.SendTime)
	// string "dl"
	o = append(o, 0xa2, 0x64, 0x6c)
	o = msgp.AppendUint(o, z.DeadLine)
	// string "data"
	o = append(o, 0xa4, 0x64, 0x61, 0x74, 0x61)
	o, err = msgp.AppendIntf(o, z.Data)
	if err != nil {
		return
	}
	// string "code"
	o = append(o, 0xa4, 0x63, 0x6f, 0x64, 0x65)
	o = msgp.AppendString(o, z.Code)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *V1Protocol) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zbzg uint32
	zbzg, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		return
	}
	for zbzg > 0 {
		zbzg--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "act":
			z.Action, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "bid":
			z.BID, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "rid":
			z.RID, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "tid":
			z.TID, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "topic":
			z.Topic, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "chan":
			z.Channel, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "nav":
			z.Nav, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "st":
			z.SendTime, bts, err = msgp.ReadUintBytes(bts)
			if err != nil {
				return
			}
		case "dl":
			z.DeadLine, bts, err = msgp.ReadUintBytes(bts)
			if err != nil {
				return
			}
		case "data":
			z.Data, bts, err = msgp.ReadIntfBytes(bts)
			if err != nil {
				return
			}
		case "code":
			z.Code, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *V1Protocol) Msgsize() (s int) {
	s = 1 + 4 + msgp.StringPrefixSize + len(z.Action) + 4 + msgp.StringPrefixSize + len(z.BID) + 4 + msgp.StringPrefixSize + len(z.RID) + 4 + msgp.StringPrefixSize + len(z.TID) + 6 + msgp.StringPrefixSize + len(z.Topic) + 5 + msgp.StringPrefixSize + len(z.Channel) + 4 + msgp.StringPrefixSize + len(z.Nav) + 3 + msgp.UintSize + 3 + msgp.UintSize + 5 + msgp.GuessSize(z.Data) + 5 + msgp.StringPrefixSize + len(z.Code)
	return
}
