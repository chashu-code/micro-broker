package sprite

import "math/rand"

// RandomSRouter 随机路由
type RandomSRouter struct {
	brokers []string
}

// NewRandomSRouter 返回随机服务路由
func NewRandomSRouter(routeMap map[string]interface{}) IServiceRouter {
	bs := routeMap["*"].(string)
	r := &RandomSRouter{
		brokers: PickBrokersFromStr(bs),
	}
	return r
}

// Route 依据 Msg，设置 Msg.Channel，进行有效路由
func (r *RandomSRouter) Route(msg *Msg, brokersAllOn []string) error {
	brokersOn := calcBrokersOn(r.brokers, brokersAllOn)
	i := rand.Intn(len(brokersOn))
	msg.Channel = brokersOn[i]
	return nil
}
