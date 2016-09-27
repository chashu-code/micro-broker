package pool

// BeanPool BeanClient pool
type BeanPool struct {
	pool chan *BeanClient
	Addr string
}

// BeanPoolWithFn With 回调
type BeanPoolWithFn func(client *BeanClient) error

// NewBeanPool 构建新的BeanPool
func NewBeanPool(addr string, size int) *BeanPool {
	return &BeanPool{
		pool: make(chan *BeanClient, size),
		Addr: addr,
	}
}

// Get 获取或构造一个 BeanClient
func (p *BeanPool) Get() *BeanClient {
	select {
	case conn := <-p.pool:
		return conn
	default:
		return NewBeanClient(p.Addr)
	}

}

// Put 推入一个 BeanClient
func (p *BeanPool) Put(conn *BeanClient) {
	if conn.LastCritical == nil {
		select {
		case p.pool <- conn:
		default:
			conn.Close()
		}
	}
}

// With 从Pool取出一个BeanClient，并回调，最终置入Pool（如果还有效的话）
func (p *BeanPool) With(fn BeanPoolWithFn) error {
	c := p.Get()
	if c.LastCritical != nil {
		return c.LastCritical
	}
	defer p.Put(c)
	return fn(c)
}
