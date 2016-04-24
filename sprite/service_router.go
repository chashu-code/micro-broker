package sprite

import (
	"fmt"
	"sort"
	"strings"
)

// IServiceRouter 路由接口
type IServiceRouter interface {
	Route(*Msg, []string) error
}

// calcBrokersOn 结合配置和线上全部broker，计算可用broker列表
func calcBrokersOn(cfg []string, all []string) []string {
	on := []string{}

	lenAll := len(all)
	// 如果全部都不上线，则默认以配置为准
	if lenAll == 0 {
		return cfg
	}

	for _, b := range cfg {
		i := sort.SearchStrings(all, b)
		if i < lenAll && all[i] == b {
			on = append(on, b)
		}
	}
	return on
}

// PickBrokersFromStr 分割字符串，提取并返回排序后的borkers列表
func PickBrokersFromStr(str string) []string {
	if len(str) == 0 {
		return []string{}
	}
	bs := strings.Split(str, ",")
	sort.Strings(bs)
	return bs
}

// PickRouterFromMap 分析Map构造适合的 IServiceRouter，无法构造返回nil
func PickRouterFromMap(hs map[string]interface{}) (sr IServiceRouter, err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("%v", v)
		}
		return
	}()

	if v, ok := hs["type"]; ok {
		var routeMap map[string]interface{}

		rtype, ok := v.(string)

		if v, ok = hs["route"]; ok {
			routeMap, _ = v.(map[string]interface{})
		}

		if len(routeMap) > 0 {
			switch rtype {
			case "random":
				sr = NewRandomSRouter(routeMap)
			}
		}
	}

	if sr == nil {
		err = fmt.Errorf("wrong route map data")
	}

	return
}
