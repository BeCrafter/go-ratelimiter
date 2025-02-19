package ratelimiter

import (
	"sync"
	"time"
)

// LimiterRecord 限流记录结构体
type LimiterRecord struct {
	Type      LimiterType // 限流器类型
	Key       string      // Redis Key
	Result    int64       // 限流结果
	Timestamp time.Time   // 执行时间
	Error     error       // 错误信息
}

// RecordHandler 记录处理接口
type RecordHandler interface {
	Handle(record LimiterRecord)
}

// 全局变量定义
var (
	recordChan   = make(chan LimiterRecord, 10000) // 限流记录通道
	handlers     = make(map[string]RecordHandler)  // 记录处理器映射
	handlerMutex sync.RWMutex                      // 限流记录处理函数互斥锁
)

func init() {
	recordChan = make(chan LimiterRecord, 10000)
	go processRecords()
}

// RegisterHandler 注册结果处理器
func RegisterHandler(name string, handler RecordHandler) {
	handlerMutex.Lock()
	defer handlerMutex.Unlock()
	handlers[name] = handler
}

// UnregisterHandler 注销结果处理器
func UnregisterHandler(name string) {
	handlerMutex.Lock()
	delete(handlers, name)
	handlerMutex.Unlock()
}

// processRecords 处理记录的协程
func processRecords() {
	for result := range recordChan {
		handlerMutex.RLock()
		// 将结果传递给所有处理器(handlers 为空时不执行此处)
		for _, handler := range handlers {
			handler.Handle(result)
		}
		handlerMutex.RUnlock()
	}
}
