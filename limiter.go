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
	currentTime time.Time       // [X] 当前时间, 单位毫秒           -- 程序内获取
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
	limitCount int64 // [V] 限流大小                    -- 参数传入
	unitTime   int64 // [V] 时间窗口大小, 单位秒，默认1秒  -- 参数传入
	expiration int64 // [-] Key 过期时间, 单位秒         -- 内部计算获得
}

// slideWindowOptions 滑动窗口限流器选项结构体
type slideWindowOptions struct {
	limitCount int64 // [V] 限流大小                    -- 参数传入
	unitTime   int64 // [V] 时间窗口大小, 单位秒，默认1秒  -- 参数传入
	expiration int64 // [-] Key 过期时间, 单位秒         -- 内部计算获得
}

// tokenBucketOptions 令牌桶限流器选项结构体
type tokenBucketOptions struct {
	maxTokens    int64 // [V] 令牌桶的上限                    -- 参数传入
	initTokens   int64 // [V] 令牌桶初始Token数量              -- 参数传入
	timeInterval int64 // [V] 桶生成时间间隔，单位秒，默认1秒    -- 参数传入
	expiration   int64 // [-] Key 过期时间，单位秒             -- 内部计算获得
}

// leakyBucketOptions 漏桶限流器选项结构体
type leakyBucketOptions struct {
	leakRate   int64 // [V] 漏水速率                    -- 参数传入
	capacity   int64 // [V] 桶的容量                    -- 参数传入
	expiration int64 // [-] Key 过期时间, 单位秒         -- 内部计算获得
}

type OptionFunc func(svr *RateLimiter)

// NewFixedWindowOption 固定窗口限流器参数设置
func NewFixedWindowOption(limitCount, unitTime int64) Options {
	return Options{
		fixedWindowOptions: fixedWindowOptions{
			limitCount: limitCount,
			unitTime:   unitTime,
		},
	}
}

// NewSlideWindowOption 滑动窗口限流器参数设置
func NewSlideWindowOption(limitCount, unitTime int64) Options {
	// 返回一个包含fixedWindowOptions结构体的Options结构体
	return Options{
		fixedWindowOptions: fixedWindowOptions{
			// 设置限流计数
			limitCount: limitCount,
			// 设置时间单位
			unitTime: unitTime,
		},
	}
}

// NewTokenBucketOption 令牌桶限流器参数设置
func NewTokenBucketOption(maxTokens, timeInterval, initTokens int64) Options {
	return Options{
		tokenBucketOptions: tokenBucketOptions{
			maxTokens:    maxTokens,
			initTokens:   initTokens,
			timeInterval: timeInterval,
		},
	}
}

// NewLeakyBucketOption 漏桶限流器参数设置
func NewLeakyBucketOption(capacity, leakRate int64) Options {
	return Options{
		leakyBucketOptions: leakyBucketOptions{
			leakRate: leakRate,
			capacity: capacity,
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
		currentTime: time.Now(), // 默认当前时间
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
		if r.options.fixedWindowOptions.expiration == 0 {
			// 默认过期时间设置为5分钟, 防止并发过高导致RedisKey被频繁删除
			r.options.fixedWindowOptions.expiration = 300
		}
	case SlideWindowType:
		r.options.slideWindowOptions = opt.slideWindowOptions
		if r.options.slideWindowOptions.expiration == 0 {
			// 当key过期时会存在瞬时并发的情况, 因此过期时间不能太短或者改用定时清除
			if r.options.slideWindowOptions.expiration <= 3600 {
				r.options.slideWindowOptions.expiration = 3600
			} else if r.options.slideWindowOptions.expiration <= 14400 {
				r.options.slideWindowOptions.expiration = r.options.slideWindowOptions.unitTime * 2
			}
		}
	case TokenBucketType:
		r.options.tokenBucketOptions = opt.tokenBucketOptions
	case LeakyBucketType:
		r.options.leakyBucketOptions = opt.leakyBucketOptions
		if r.options.leakyBucketOptions.expiration == 0 {
			if r.options.leakyBucketOptions.expiration >= 14400 {
				r.options.leakyBucketOptions.expiration = 14400
			} else if r.options.leakyBucketOptions.expiration <= 3600 {
				r.options.leakyBucketOptions.expiration = 3600
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
		r.options.fixedWindowOptions.limitCount,
		r.options.fixedWindowOptions.unitTime,
		r.options.fixedWindowOptions.expiration,
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
		r.options.slideWindowOptions.limitCount,
		r.currentTime.UnixMilli(),
		r.options.slideWindowOptions.unitTime,
		r.options.slideWindowOptions.expiration,
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
	bucketMaxTokens := r.options.tokenBucketOptions.maxTokens
	// 限流时间间隔 -- 对应时间窗口
	resetBucketInterval := cast.ToInt64(r.options.tokenBucketOptions.timeInterval * 1000)
	// 令牌的产生间隔 = 限流时间 / 最大令牌数
	intervalPerPermit := int64(1)
	if resetBucketInterval > bucketMaxTokens {
		intervalPerPermit = cast.ToInt64(math.Ceil(float64(resetBucketInterval) / float64(bucketMaxTokens)))
	}
	// 初始令牌数
	initTokens := r.options.tokenBucketOptions.initTokens
	// 用 最大的突发流量的持续时间 计算的结果更加合理,并不是每次初始化都要将桶装满
	if initTokens > bucketMaxTokens {
		initTokens = bucketMaxTokens
	}

	options := []interface{}{
		intervalPerPermit,
		r.currentTime.UnixMilli(),
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
		r.options.leakyBucketOptions.capacity, // 桶的容量
		r.options.leakyBucketOptions.leakRate, // 漏水速率, 单位是每秒漏多少个请求
		r.currentTime.Unix(),                  // 单位秒
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
		limitCount = r.options.fixedWindowOptions.limitCount
		// 固定窗口类型需要添加时间戳后缀
		suffix = cast.ToString(math.Floor(float64(r.currentTime.Unix()) / float64(r.options.fixedWindowOptions.unitTime)))
	case SlideWindowType: // 固定KEY，无后缀
		limitCount = r.options.slideWindowOptions.limitCount
	case TokenBucketType: // 固定KEY，无后缀
		limitCount = r.options.tokenBucketOptions.maxTokens
	case LeakyBucketType: // 固定KEY，无后缀
		limitCount = r.options.leakyBucketOptions.leakRate
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
