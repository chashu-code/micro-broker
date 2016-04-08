package fsm

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func buildFlowDescList() FlowDescList {
	return FlowDescList{
		{Evt: "one", SrcList: []string{"start"}, Dst: "want_two"},
		{Evt: "two", SrcList: []string{"want_two"}, Dst: "want_three"},
		{Evt: "three", SrcList: []string{"want_three"}, Dst: "end"},
	}
}

func buildCbDesc(isEmpty bool) CallbackDesc {
	if isEmpty {
		return CallbackDesc{}
	}

	fnStateChange := func(key CallbackKey, evt *Event) error {
		// fmt.Printf("state change: on %q, recv %q\n", key, evt)
		return nil
	}

	return CallbackDesc{
		"enter-*": fnStateChange,
		"leave-*": fnStateChange,
		"stop":    fnStateChange,
	}
}

func Benchmark_Flow(b *testing.B) {
	opts := map[string]interface{}{
		"name":         "test",
		"initState":    "start",
		"flowDesc":     buildFlowDescList(),
		"callbackDesc": buildCbDesc(true),
	}
	fsm := New(opts)
	go fsm.Serve()

	for i := 0; i < b.N; i++ {
		evt := Event{
			Name: "one",
		}
		fsm.Flow(evt)
	}
}

func Test_NormalFlow(t *testing.T) {
	opts := map[string]interface{}{
		"name":         "test",
		"initState":    "start",
		"flowDesc":     buildFlowDescList(),
		"callbackDesc": buildCbDesc(false),
	}
	fsm := New(opts)
	go fsm.Serve()

	names := []string{"one", "two", "three"}
	for _, name := range names {
		evt := Event{
			Name: name,
		}
		fsm.Flow(evt)

	}

	time.Sleep(1 * time.Millisecond)
	assert.Equal(t, "end", fsm.Current())

}

func Test_FlowEvtNext(t *testing.T) {
	cbDesc := buildCbDesc(false)

	cbDesc["enter-start"] = func(_ CallbackKey, evt *Event) error {
		evt.Next = &Event{
			Name: "one",
		}
		return nil
	}

	opts := map[string]interface{}{
		"name":         "test",
		"initState":    "start",
		"flowDesc":     buildFlowDescList(),
		"callbackDesc": cbDesc,
	}
	fsm := New(opts)
	go fsm.Serve()

	names := []string{"two", "three"}
	for _, name := range names {
		evt := Event{
			Name: name,
		}
		fsm.Flow(evt)

	}

	time.Sleep(1 * time.Millisecond)
	assert.Equal(t, "end", fsm.Current())

}

func Test_FailFlow(t *testing.T) {
	opts := map[string]interface{}{
		"name":         "test",
		"initState":    "start",
		"flowDesc":     buildFlowDescList(),
		"callbackDesc": buildCbDesc(false),
	}
	fsm := New(opts)
	go fsm.Serve()

	names := []string{"one", "three", "three"}
	for _, name := range names {
		evt := Event{
			Name: name,
		}
		fsm.Flow(evt)
	}

	time.Sleep(1 * time.Millisecond)
	assert.NotEqual(t, "end", fsm.Current())
	assert.Equal(t, "want_two", fsm.Current())
}

func Test_StopFSMwithEvt(t *testing.T) {
	cbDesc := buildCbDesc(false)

	var hasStop = make(chan bool)
	cbDesc["stop"] = func(_ CallbackKey, evt *Event) error {
		err := evt.Args[0].(error)
		assert.Equal(t, err, ErrStopWithEvt)
		hasStop <- true
		return nil
	}

	opts := map[string]interface{}{
		"name":         "test",
		"initState":    "start",
		"flowDesc":     buildFlowDescList(),
		"callbackDesc": cbDesc,
	}
	fsm := New(opts)
	go fsm.Serve()

	evt := Event{
		Name: EvtNameStopFSM,
	}
	fsm.Flow(evt)

	<-hasStop
}

func Test_StopFSMwithPanic(t *testing.T) {
	cbDesc := buildCbDesc(false)

	var hasStop = make(chan bool)
	cbDesc["stop"] = func(_ CallbackKey, evt *Event) error {
		err := evt.Args[0].(string)
		assert.Equal(t, err, "sorry")
		hasStop <- true
		return nil
	}
	cbDesc["enter-*"] = func(_ CallbackKey, _ *Event) error {
		panic("sorry")
	}

	opts := map[string]interface{}{
		"name":         "test",
		"initState":    "start",
		"flowDesc":     buildFlowDescList(),
		"callbackDesc": cbDesc,
	}
	fsm := New(opts)
	go fsm.Serve()

	<-hasStop
}

func Test_FlowCallback(t *testing.T) {
	var hasEnter = make(chan bool, 3)
	cbDesc := buildCbDesc(false)
	cbDesc["leave-*"] = func(_ CallbackKey, evt *Event) error {
		assert.Equal(t, evt.Service, "test")
		hasEnter <- true
		return nil
	}
	cbDesc["enter-want_three"] = func(_ CallbackKey, _ *Event) error {
		hasEnter <- true
		return errors.New("sorry")
	}

	opts := map[string]interface{}{
		"name":         "test",
		"initState":    "start",
		"flowDesc":     buildFlowDescList(),
		"callbackDesc": cbDesc,
	}
	fsm := New(opts)
	go fsm.Serve()

	evt := Event{
		Name: "one",
	}
	fsm.Flow(evt)

	evt.Name = "two"
	fsm.Flow(evt)

	<-hasEnter
	<-hasEnter
	<-hasEnter
	assert.Equal(t, "want_two", fsm.Current())
}
