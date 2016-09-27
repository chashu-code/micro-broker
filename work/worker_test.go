package work

import (
	"testing"
	"time"

	"github.com/chashu-code/micro-broker/manage"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/zap"
	"github.com/uber-go/zap/spy"
)

func newMsgBytes(v uint, deadLine int64, mgr *manage.Manager) []byte {
	msg := &manage.Msg{
		V:        1,
		DeadLine: deadLine,
	}

	bts, _ := mgr.Pack(msg)
	bts[0] = byte(v)
	return bts
}

func (w *Worker) newSinkLog() *spy.Sink {
	var sink *spy.Sink
	w.Log, sink = spy.New()
	return sink
}

func logHas(t *testing.T, sink *spy.Sink, msgs ...string) {
	isLogHash(true, t, sink, msgs...)
}

func logNotHas(t *testing.T, sink *spy.Sink, msgs ...string) {
	isLogHash(false, t, sink, msgs...)
}

func isLogHash(isHas bool, t *testing.T, sink *spy.Sink, msgs ...string) {
	b := &testBuffer{}
	for _, l := range sink.Logs() {
		enc := zap.NewJSONEncoder()
		for _, field := range l.Fields {
			field.AddTo(enc)
		}
		enc.WriteEntry(b, l.Msg, l.Level, time.Now())
		enc.Free()
	}

	log := b.String()
	for _, msg := range msgs {
		if isHas {
			assert.Contains(t, log, msg)
		} else {
			assert.NotContains(t, log, msg)
		}

	}
}
