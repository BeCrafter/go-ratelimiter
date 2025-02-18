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
	LimitCount int64 // [V] 限流大小                    -- 参数传入
	TimeRange  int64 // [V] 时间窗口大小, 单位秒, 默认1秒  -- 参数传入
	Expiration int64 // [-] Key 过期时间                -- 内部计算获得
	InitTokens int64 // [-] 令牌桶初始Token数量
	Capacity   int64 // [-] 令牌桶容量                  -- 参数传入
}

type OptionFunc func(svr *RateLimiter)

// NewRateLimiter 限流器实例化
func NewRateLimiter(product string, limiterType LimiterType, opts ...OptionFunc) *RateLimiter {
	return &RateLimiter{
		ctx:         context.TODO(),
		product:     product,
		client:      redisClient,
		limiterType: limiterType,
		currentTime: time.Now().UnixMilli(), // ms
		optionFuncs: opts,
	}
}

// WithContext 上下文设置
func (r *RateLimiter) WithContext(ctx context.Context) *RateLimiter {
	r.ctx = ctx
	return r
}

// WithOptionFunc 自定义拓展函数设置
func (r *RateLimiter) WithOptionFunc(ops ...OptionFunc) *RateLimiter {
	if len(ops) > 0 {
		r.optionFuncs = append(r.optionFuncs, ops...)
	}
	return r
}

// SetOptions 设置限流器参数
func (r *RateLimiter) WithOption(opt Options) *RateLimiter {
	if opt.LimitCount == 0 {
		opt.LimitCount = 1
	}
	if opt.TimeRange == 0 {
		opt.TimeRange = 1
	}

	// Expiration 为空时，程序自动补充过期时间（仅对 固定窗口 / 滑动窗口 生效）
	if opt.Expiration == 0 {
		switch r.limiterType {
		case FixedWindowType:
			// 过期时间范围：300 ~ 600s
			opt.Expiration = opt.TimeRange * 2
			if opt.Expiration > 600 {
				opt.Expiration = 600
			} else if opt.Expiration < 300 {
				opt.Expiration = 300
			}
		case SlideWindowType:
			// 当key过期时会存在瞬时并发的情况, 因此过期时间不能太短或者改用定时清除
			if opt.Expiration <= 3600 {
				opt.Expiration = 3600
			} else if opt.Expiration <= 14400 {
				opt.Expiration = opt.TimeRange * 2
			}
		case TokenBucketType:
			// Lua 脚本中根据 TimeRange 自动计算
		case LeakyBucketType:
			if opt.Expiration <= 3600 {
				opt.Expiration = 3600
			} else if opt.Expiration <= 14400 {
				opt.Expiration = opt.TimeRange * 2
			}
		}
	}

	r.options = opt

	// 用户自定义 RedisKey 优先级最高
	if len(r.redisKey) == 0 {
		r.redisKey = r.genLimiterKey()
	}

	return r
}

// SetRedisKey 支持自定义设置RedisKey
func (r *RateLimiter) SetRedisKey(key string) *RateLimiter {
	if len(key) > 0 {
		r.redisKey = key
	}

	return r
}

// GetRedisKey 输出RedisKey
func (r *RateLimiter) GetRedisKey() string {
	return r.redisKey
}

// Do 执行限流器
func (r *RateLimiter) Do() (ret int64, err error) {
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
		r.options.LimitCount,
		r.options.TimeRange,
		r.options.Expiration,
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
		r.options.LimitCount,
		r.currentTime,
		r.options.TimeRange,
		r.options.Expiration,
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
	bucketMaxTokens := r.options.LimitCount
	// 限流时间间隔 -- 对应时间窗口
	resetBucketInterval := r.options.TimeRange * 1000
	// 令牌的产生间隔 = 限流时间 / 最大令牌数
	intervalPerPermit := int64(1)
	if resetBucketInterval > bucketMaxTokens {
		intervalPerPermit = cast.ToInt64(math.Ceil(float64(resetBucketInterval) / float64(bucketMaxTokens)))
	}
	// 初始令牌数
	initTokens := r.options.InitTokens
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
		r.options.Capacity,   // 桶的容量
		r.options.LimitCount, // 漏水速率, 单位是每秒漏多少个请求
		r.currentTime / 1000, // 单位秒
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
	suffix := ""
	switch r.limiterType {
	case FixedWindowType: // 以时间戳作为后缀
		suffix = cast.ToString(math.Floor(float64(r.currentTime) / 1000 / float64(r.options.TimeRange)))
		fallthrough
	case SlideWindowType: // 固定KEY，无后缀
		fallthrough
	case TokenBucketType: // 固定KEY，无后缀
		fallthrough
	case LeakyBucketType: // 固定KEY，无后缀
		fallthrough
	default: // 执行默认拆KEY操作，防止热KEY场景
		mod := 0
		if r.options.LimitCount > MaxBucketCapacity {
			tmp := math.Ceil(float64(r.options.LimitCount) / float64(MaxBucketCapacity))
			mod = time.Now().Nanosecond() % cast.ToInt(tmp)
		}

		if len(suffix) == 0 {
			suffix = cast.ToString(mod)
		} else {
			suffix += "::" + cast.ToString(mod)
		}
	}

	ret := RedisKeyPrefix + "::" + string(r.limiterType) + "::" + r.product
	if len(suffix) > 0 {
		ret += "::" + suffix
	}

	return ret
}
