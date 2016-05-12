package manage

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

// Broker 代理服务
type Broker struct {
	Manager
}

// NewBroker 构造一个新的代理Broker
func NewBroker(name string, pathConfig string) *Broker {
	b := new(Broker)
	b.init(name)
	b.updateWithConfFile(pathConfig)
	return b
}

func (m *Broker) updateWithConfFile(path string) {
	if path != "" {
		data, err := ioutil.ReadFile(path)
		if err != nil {
			panic(fmt.Errorf("read config fail: %v", err))
		}

		conf := make(map[string]interface{})
		if err = json.Unmarshal(data, &conf); err != nil {
			panic(fmt.Errorf("config decode fail: %v", err))
		}

		m.updateWithSyncInfo(conf)
	}

}
