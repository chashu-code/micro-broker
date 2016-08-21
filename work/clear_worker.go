package work

// ClearWorker 清理工作器
type ClearWorker struct {
	Worker
}

// clearResQueue 清理redis应答队列（超过1min无变化）
func (w *ClearWorker) clearResQueue() {

}
