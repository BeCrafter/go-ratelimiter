package ratelimiter

import (
	"log"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// LogHandler 日志处理器
type LogHandler struct {
	records []LimiterRecord
	mu      sync.Mutex
}

func NewLogHandler() *LogHandler {
	return &LogHandler{
		records: make([]LimiterRecord, 0),
	}
}

func (h *LogHandler) Handle(record LimiterRecord) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, record)
	log.Printf("Limiter[%s] Key[%s] Result[%d] Error[%v]",
		record.Type, record.Key, record.Result, record.Error)
}

func (h *LogHandler) GetRecords() []LimiterRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.records
}

func TestRecordHandler(t *testing.T) {
	tests := []struct {
		name        string
		limiterType LimiterType
		options     Options
		wantResult  int64
		wantError   bool
	}{
		{
			name:        "固定窗口限流-正常",
			limiterType: FixedWindowType,
			options:     NewFixedWindowOption(10, 1),
			wantResult:  10,
			wantError:   false,
		},
		{
			name:        "滑动窗口限流-正常",
			limiterType: SlideWindowType,
			options:     NewSlideWindowOption(10, 1),
			wantResult:  0,
			wantError:   false,
		},
		{
			name:        "令牌桶限流-正常",
			limiterType: TokenBucketType,
			options:     NewTokenBucketOption(10, 1, 5),
			wantResult:  5,
			wantError:   false,
		},
		{
			name:        "漏桶限流-正常",
			limiterType: LeakyBucketType,
			options:     NewLeakyBucketOption(10, 1),
			wantResult:  1,
			wantError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 准备测试环境
			handler := NewLogHandler()
			RegisterHandler(tt.name, handler)
			defer UnregisterHandler(tt.name)

			// 执行限流
			limiter := NewRateLimiter("test", tt.limiterType, tt.options)
			result, err := limiter.Do()

			// 等待异步处理完成
			time.Sleep(100 * time.Millisecond)

			// 验证结果
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantResult, result)
			}

			// 验证记录
			records := handler.GetRecords()
			assert.NotEmpty(t, records)
			assert.Equal(t, tt.limiterType, records[0].Type)
			assert.Equal(t, result, records[0].Result)
		})
	}
}

func TestRecordHandlerConcurrent(t *testing.T) {
	handler := NewLogHandler()
	RegisterHandler("concurrent", handler)
	defer UnregisterHandler("concurrent")

	// 并发测试
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			limiter := NewRateLimiter("test", FixedWindowType, NewFixedWindowOption(1000, 1))
			limiter.Do()
		}()
	}
	wg.Wait()

	// 等待异步处理完成
	time.Sleep(200 * time.Millisecond)

	// 验证记录
	records := handler.GetRecords()
	assert.NotEmpty(t, records)
	assert.GreaterOrEqual(t, len(records), 100)
}

func TestRecordHandlerChannelFull(t *testing.T) {
	handler := NewLogHandler()
	RegisterHandler("full", handler)
	defer UnregisterHandler("full")

	// 模拟通道满的情况
	for i := 0; i < 20000; i++ {
		limiter := NewRateLimiter("test", FixedWindowType, NewFixedWindowOption(1, 1))
		limiter.Do()
	}

	// 等待部分处理完成
	time.Sleep(100 * time.Millisecond)

	// 验证有记录被处理
	records := handler.GetRecords()
	assert.NotEmpty(t, records)
}
