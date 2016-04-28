package manage

// Broker 代理服务
type Broker struct {
	Manager
}

// NewBroker 构造一个新的代理Broker
func NewBroker(name string) *Broker {
	b := new(Broker)
	b.init(name)
	return b
}
