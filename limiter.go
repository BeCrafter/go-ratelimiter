// Copyright(C) 2024 Github Inc. All Rights Reserved.
// Author: metrue8@gmail.com
// Date:   2024/01/03

package ratelimiter

import (
	"context"
	"math"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
)

// LimiterType 定义限流器类型
type LimiterType string

const (
	// RedisKeyPrefix 限流器 Redis Key 存储前缀
	RedisKeyPrefix string = "dlimiter"

	// MaxBucketCapacity Redis 单片分库最大请求容量限制
	MaxBucketCapacity int64 = 5000
)

// 定义限流器类型常量
const (
	FixedWindowType LimiterType = "FixedWindow" // 固定窗口限流器
	SlideWindowType LimiterType = "SlideWindow" // 滑动窗口限流器
	TokenBucketType LimiterType = "TokenBucket" // 令牌桶限流器
	LeakyBucketType LimiterType = "LeakyBucket" // 漏桶限流器
)

// RateLimiter 定义限流器结构体
type RateLimiter struct {
	ctx         context.Context // [V] 上下文
	product     string          // [V] 业务线
	client      *redis.Client   // [V] Redis 客户端
	limiterType LimiterType     // [V] 限流器类型
	redisKey    string          // [X] 存储Key                    -- 内部计算获得
	currentTime int64           // [X] 当前时间, 单位毫秒           -- 程序内获取
	options     Options         // [-] 限流器参数
	optionFuncs []OptionFunc    // [-] 自定义拓展函数
}

// Option 限流器参数
type Options struct {
	fixedWindowOptions fixedWindowOptions // 固定窗口限流器选项
	slideWindowOptions slideWindowOptions // 滑动窗口限流器选项
	tokenBucketOptions tokenBucketOptions // 令牌桶限流器选项
	leakyBucketOptions leakyBucketOptions // 漏桶限流器选项
}

// fixedWindowOptions 固定窗口限流器选项结构体
type fixedWindowOptions struct {
	LimitCount int64 // [V] 限流大小                    -- 参数传入
	TimeRange  int64 // [V] 时间窗口大小, 单位秒, 默认1秒  -- 参数传入
	Expiration int64 // [-] Key 过期时间                -- 内部计算获得
	InitTokens int64 // [-] 令牌桶初始Token数量
	Capacity   int64 // [-] 令牌桶容量                  -- 参数传入
}

// slideWindowOptions 滑动窗口限流器选项结构体
type slideWindowOptions struct {
	LimitCount int64 // [V] 限流大小                    -- 参数传入
	TimeRange  int64 // [V] 时间窗口大小, 单位秒, 默认1秒  -- 参数传入
	Expiration int64 // [-] Key 过期时间                -- 内部计算获得
	InitTokens int64 // [-] 令牌桶初始Token数量
	Capacity   int64 // [-] 令牌桶容量                  -- 参数传入
}

// tokenBucketOptions 令牌桶限流器选项结构体
type tokenBucketOptions struct {
	LimitCount int64 // [V] 限流大小                    -- 参数传入
	TimeRange  int64 // [V] 时间窗口大小, 单位秒, 默认1秒  -- 参数传入
	Expiration int64 // [-] Key 过期时间                -- 内部计算获得
	InitTokens int64 // [-] 令牌桶初始Token数量
	Capacity   int64 // [-] 令牌桶容量                  -- 参数传入
}

// leakyBucketOptions 漏桶限流器选项结构体
type leakyBucketOptions struct {
	LimitCount int64 // [V] 限流大小                    -- 参数传入
	TimeRange  int64 // [V] 时间窗口大小, 单位秒, 默认1秒  -- 参数传入
	Expiration int64 // [-] Key 过期时间                -- 内部计算获得
	InitTokens int64 // [-] 令牌桶初始Token数量
	Capacity   int64 // [-] 令牌桶容量                  -- 参数传入
}

type OptionFunc func(svr *RateLimiter)

// NewFixedWindowOption 固定窗口限流器参数设置
func NewFixedWindowOption(limitCount, timeRange int64) Options {
	return Options{
		fixedWindowOptions: fixedWindowOptions{
			LimitCount: limitCount,
			TimeRange:  timeRange,
		},
	}
}

// NewSlideWindowOption 滑动窗口限流器参数设置
func NewSlideWindowOption(limitCount, timeRange int64) Options {
	return Options{
		fixedWindowOptions: fixedWindowOptions{
			LimitCount: limitCount,
			TimeRange:  timeRange,
		},
	}
}

// NewTokenBucketOption 令牌桶限流器参数设置
func NewTokenBucketOption(limitCount, timeRange int64) Options {
	return Options{
		tokenBucketOptions: tokenBucketOptions{
			LimitCount: limitCount,
			TimeRange:  timeRange,
		},
	}
}

// NewLeakyBucketOption 漏桶限流器参数设置
func NewLeakyBucketOption(limitCount, timeRange int64) Options {
	return Options{
		leakyBucketOptions: leakyBucketOptions{
			LimitCount: limitCount,
			TimeRange:  timeRange,
		},
	}
}

// NewRateLimiter 限流器实例化
func NewRateLimiter(product string, limiterType LimiterType, ops ...Options) *RateLimiter {
	limiter := &RateLimiter{
		ctx:         context.TODO(),
		product:     product,
		client:      redisClient,
		limiterType: limiterType,
		currentTime: time.Now().UnixMilli(), // ms
	}

	if len(ops) > 0 {
		limiter.options = ops[0]
	}

	return limiter
}

// WithContext 上下文设置
func (r *RateLimiter) WithContext(ctx context.Context) *RateLimiter {
	r.ctx = ctx
	return r
}

// WithOptionFunc 自定义拓展函数设置
func (r *RateLimiter) WithOptionFunc(ops ...OptionFunc) *RateLimiter {
	if len(ops) > 0 {
		r.optionFuncs = ops
	}
	return r
}

// WithOption 设置限流器参数
func (r *RateLimiter) WithOption(opt Options) *RateLimiter {
	r.options = opt
	return r
}

// WithRedisKey 支持自定义设置RedisKey
func (r *RateLimiter) WithRedisKey(key string) *RateLimiter {
	if len(key) > 0 {
		r.redisKey = key
	}

	return r
}

// initOptions 初始化限流器参数
func (r *RateLimiter) initOptions(opt Options) error {
	switch r.limiterType {
	case FixedWindowType:
		r.options.fixedWindowOptions = opt.fixedWindowOptions
		if r.options.fixedWindowOptions.LimitCount == 0 {
			r.options.fixedWindowOptions.LimitCount = 1
		}
		if r.options.fixedWindowOptions.TimeRange == 0 {
			r.options.fixedWindowOptions.TimeRange = 1
		}
		if r.options.fixedWindowOptions.Expiration == 0 {
			// 过期时间范围：300 ~ 600s
			r.options.fixedWindowOptions.Expiration = r.options.fixedWindowOptions.TimeRange * 2
			if r.options.fixedWindowOptions.Expiration > 600 {
				r.options.fixedWindowOptions.Expiration = 600
			} else if r.options.fixedWindowOptions.Expiration < 300 {
				r.options.fixedWindowOptions.Expiration = 300
			}
		}
	case SlideWindowType:
		r.options.slideWindowOptions = opt.slideWindowOptions
		if r.options.slideWindowOptions.LimitCount == 0 {
			r.options.slideWindowOptions.LimitCount = 1
		}
		if r.options.slideWindowOptions.TimeRange == 0 {
			r.options.slideWindowOptions.TimeRange = 1
		}
		if r.options.slideWindowOptions.Expiration == 0 {
			// 当key过期时会存在瞬时并发的情况, 因此过期时间不能太短或者改用定时清除
			if r.options.slideWindowOptions.Expiration <= 3600 {
				r.options.slideWindowOptions.Expiration = 3600
			} else if r.options.slideWindowOptions.Expiration <= 14400 {
				r.options.slideWindowOptions.Expiration = r.options.slideWindowOptions.TimeRange * 2
			}
		}
	case TokenBucketType:
		r.options.tokenBucketOptions = opt.tokenBucketOptions
		if r.options.tokenBucketOptions.LimitCount == 0 {
			r.options.tokenBucketOptions.LimitCount = 1
		}
		if r.options.tokenBucketOptions.TimeRange == 0 {
			r.options.tokenBucketOptions.TimeRange = 1
		}
		if r.options.tokenBucketOptions.Expiration == 0 {
			// Lua 脚本中根据 TimeRange 自动计算
		}
	case LeakyBucketType:
		r.options.leakyBucketOptions = opt.leakyBucketOptions
		if r.options.leakyBucketOptions.LimitCount == 0 {
			r.options.leakyBucketOptions.LimitCount = 1
		}
		if r.options.leakyBucketOptions.TimeRange == 0 {
			r.options.leakyBucketOptions.TimeRange = 1
		}
		if r.options.leakyBucketOptions.Expiration == 0 {
			if r.options.leakyBucketOptions.Expiration <= 3600 {
				r.options.leakyBucketOptions.Expiration = 3600
			} else if r.options.leakyBucketOptions.Expiration <= 14400 {
				r.options.leakyBucketOptions.Expiration = r.options.leakyBucketOptions.TimeRange * 2
			}
		}
	}

	// 用户自定义 RedisKey 优先级最高
	if len(r.redisKey) == 0 {
		r.redisKey = r.genLimiterKey()
	}

	return nil
}

// GetRedisKey 输出RedisKey
func (r *RateLimiter) GetRedisKey() string {
	return r.redisKey
}

// Do 执行限流器
func (r *RateLimiter) Do() (ret int64, err error) {
	if err := r.initOptions(r.options); err != nil {
		return 0, err
	}

	switch r.limiterType {
	case FixedWindowType:
		ret, err = r.doFixedWindowLimiter()
	case SlideWindowType:
		ret, err = r.doSlideWindowLimiter()
	case TokenBucketType:
		ret, err = r.doTokenBucketLimiter()
	case LeakyBucketType:
		ret, err = r.doLeakyBucketLimiter()
	}

	// 执行自定义拓展函数
	for _, fn := range r.optionFuncs {
		fn(r)
	}

	return ret, err
}

// doFixedWindowLimiter 执行固定窗口限流
func (r *RateLimiter) doFixedWindowLimiter() (int64, error) {
	options := []interface{}{
		r.options.fixedWindowOptions.LimitCount,
		r.options.fixedWindowOptions.TimeRange,
		r.options.fixedWindowOptions.Expiration,
	}
	res, err := EvalSha(r.ctx, r.client, r.getScriptSha(), []string{r.redisKey}, options...)

	// 脚本缓存丢失时执行一次使用脚本重查
	if err != nil && err.Error() == NoScriptMsg {
		res, err = Eval(r.ctx, r.client, r.getScript(), []string{r.redisKey}, options...)
	}

	if err != nil {
		return 0, err
	}

	return cast.ToInt64(res), nil
}

// doSlideWindowLimiter 执行滑动窗口限流
func (r *RateLimiter) doSlideWindowLimiter() (int64, error) {
	options := []interface{}{
		r.options.slideWindowOptions.LimitCount,
		r.currentTime,
		r.options.slideWindowOptions.TimeRange,
		r.options.slideWindowOptions.Expiration,
	}
	res, err := EvalSha(r.ctx, r.client, r.getScriptSha(), []string{r.redisKey}, options...)

	// 脚本缓存丢失时执行一次使用脚本重查
	if err != nil && err.Error() == NoScriptMsg {
		res, err = Eval(r.ctx, r.client, r.getScript(), []string{r.redisKey}, options...)
	}

	if err != nil {
		return 0, err
	}

	return cast.ToInt64(res), nil
}

// doTokenBucketLimiter 执行令牌桶限流
func (r *RateLimiter) doTokenBucketLimiter() (int64, error) {
	// 最大令牌数   -- 对应限流大小
	bucketMaxTokens := r.options.tokenBucketOptions.LimitCount
	// 限流时间间隔 -- 对应时间窗口
	resetBucketInterval := r.options.tokenBucketOptions.TimeRange * 1000
	// 令牌的产生间隔 = 限流时间 / 最大令牌数
	intervalPerPermit := int64(1)
	if resetBucketInterval > bucketMaxTokens {
		intervalPerPermit = cast.ToInt64(math.Ceil(float64(resetBucketInterval) / float64(bucketMaxTokens)))
	}
	// 初始令牌数
	initTokens := r.options.tokenBucketOptions.InitTokens
	// 用 最大的突发流量的持续时间 计算的结果更加合理,并不是每次初始化都要将桶装满
	if initTokens > bucketMaxTokens {
		initTokens = bucketMaxTokens
	}

	options := []interface{}{
		intervalPerPermit,
		r.currentTime,
		bucketMaxTokens,
		resetBucketInterval,
		initTokens,
	}
	res, err := EvalSha(r.ctx, r.client, r.getScriptSha(), []string{r.redisKey}, options...)

	// 脚本缓存丢失时执行一次使用脚本重查
	if err != nil && err.Error() == NoScriptMsg {
		res, err = Eval(r.ctx, r.client, r.getScript(), []string{r.redisKey}, options...)
	}

	if err != nil {
		return 0, err
	}

	return cast.ToInt64(res), nil
}

// doLeakyBucketLimiter 执行漏桶限流
func (r *RateLimiter) doLeakyBucketLimiter() (int64, error) {
	options := []interface{}{
		r.options.leakyBucketOptions.Capacity,   // 桶的容量
		r.options.leakyBucketOptions.LimitCount, // 漏水速率, 单位是每秒漏多少个请求
		r.currentTime / 1000,                    // 单位秒
	}
	res, err := EvalSha(r.ctx, r.client, r.getScriptSha(), []string{r.redisKey}, options...)

	// 脚本缓存丢失时执行一次使用脚本重查
	if err != nil && err.Error() == NoScriptMsg {
		res, err = Eval(r.ctx, r.client, r.getScript(), []string{r.redisKey}, options...)
	}

	if err != nil {
		return 0, err
	}

	return cast.ToInt64(res), nil
}

// getScript 获取限流器执行脚本
func (r *RateLimiter) getScript() (script string) {
	return getLuaScript(r.limiterType, compressFlag)
}

// getScriptSha 获取限流器执行脚本Sha值
func (r *RateLimiter) getScriptSha() (sha1 string) {
	switch r.limiterType {
	case FixedWindowType:
		sha1 = ScriptShas.FixedWindow
	case SlideWindowType:
		sha1 = ScriptShas.SlideWindow
	case TokenBucketType:
		sha1 = ScriptShas.TokenBucket
	case LeakyBucketType:
		sha1 = ScriptShas.LeakyBucket
	}

	if sha1 == "" {
		if ok := ScriptFlush(r.ctx, r.client); ok {
			loadRedisScript(r.client)
		}
	}

	return sha1
}

// genLimiterKey 生成存储Key
func (r *RateLimiter) genLimiterKey() string {
	var (
		suffix     string
		limitCount int64
	)

	// 获取对应限流器类型的 LimitCount
	switch r.limiterType {
	case FixedWindowType: // 以时间戳作为后缀
		limitCount = r.options.fixedWindowOptions.LimitCount
		// 固定窗口类型需要添加时间戳后缀
		suffix = cast.ToString(math.Floor(float64(r.currentTime) / 1000 / float64(r.options.fixedWindowOptions.TimeRange)))
	case SlideWindowType: // 固定KEY，无后缀
		limitCount = r.options.slideWindowOptions.LimitCount
	case TokenBucketType: // 固定KEY，无后缀
		limitCount = r.options.tokenBucketOptions.LimitCount
	case LeakyBucketType: // 固定KEY，无后缀
		limitCount = r.options.leakyBucketOptions.LimitCount
	}

	// 处理大容量限流的情况，防止热Key
	mod := 0
	if limitCount > MaxBucketCapacity {
		tmp := math.Ceil(float64(limitCount) / float64(MaxBucketCapacity))
		mod = time.Now().Nanosecond() % cast.ToInt(tmp)
	}

	if len(suffix) == 0 {
		suffix = cast.ToString(mod)
	} else {
		suffix += "::" + cast.ToString(mod)
	}

	ret := RedisKeyPrefix + "::" + string(r.limiterType) + "::" + r.product
	if len(suffix) > 0 {
		ret += "::" + suffix
	}

	return ret
}
