// Copyright(C) 2024 Github Inc. All Rights Reserved.
// Author: metrue8@gmail.com
// Date:   2024/01/03

package ratelimiter

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	once         sync.Once
	client       *redis.Client
	redisProcess *os.Process
)

func init() {
	once.Do(func() {
		// 启动 Redis
		// if err := startRedis(); err != nil {
		// 	fmt.Printf("Redis 启动失败: %v\n", err)
		// 	os.Exit(1)
		// }

		client = redis.NewClient(&redis.Options{
			Addr:     "localhost:6379",
			Password: "", // no password set
			DB:       0,  // use default DB
		})

		Init(client, true)
		// 停止 Redis
		// stopRedis()
	})

}

func startRedis() error {
	// 启动 Redis 服务器
	cmd := exec.Command("redis-server", "--port", "6379", "--save", "\"\"", "--appendonly", "no")
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("启动 Redis 失败: %v", err)
	}
	redisProcess = cmd.Process

	// 等待 Redis 启动完成
	time.Sleep(2 * time.Second)
	return nil
}

func stopRedis() {
	if redisProcess != nil {
		redisProcess.Kill()
		redisProcess.Wait()
	}
}

func TestLimiter_LoadScript(t *testing.T) {
	sha, err := LoadScript(context.TODO(), client, luaScriptMap["FixedWindowScript"])
	if err != nil {
		t.Errorf("LoadScript fail, script[%s] err[%+v]", luaScriptMap["FixedWindowScript"], err)
	} else {
		t.Logf("sha1_value[%s]", sha)
	}
}

func TestLimiter_FixedWindowLimiter(t *testing.T) {
	sha, err := LoadScript(context.TODO(), client, luaScriptMap["FixedWindowScript"])
	if err != nil {
		t.Errorf("LoadScript fail, script[%s] err[%+v]", luaScriptMap["FixedWindowScript"], err)
	}

	options := []interface{}{
		3,
		1,
		1,
	}
	curtime := time.Now().Unix()
	for i := 0; i < 4; i++ {
		func(cur int) {
			val, err := EvalSha(context.TODO(), client, sha, []string{"test_yy_test111111"}, options...)
			if err != nil {
				t.Errorf("EvalSha fail, script[%s] err[%+v]", luaScriptMap["FixedWindowScript"], err)
			} else {
				t.Logf("Run succ, cur[%v] ret[%v]", cur, val)
			}
		}(int(curtime) + i)
	}

	for i := 0; i < 4; i++ {
		func(cur int) {
			val, err := EvalSha(context.TODO(), client, sha, []string{"test_yy_test111111"}, options...)
			if err != nil {
				t.Errorf("EvalSha fail, script[%s] err[%+v]", luaScriptMap["FixedWindowScript"], err)
			} else {
				t.Logf("Run succ, cur[%v] ret[%v]", time.Now().Unix(), val)
			}
		}(int(curtime) + 5)
	}
}

func TestLimiter_SlideWindowLimiter(t *testing.T) {
	sha, err := LoadScript(context.TODO(), client, luaScriptMap["FixedWindowScript"])
	if err != nil {
		t.Errorf("LoadScript fail, script[%s] err[%+v]", luaScriptMap["FixedWindowScript"], err)
	}

	curtime := time.Now().UnixMilli()
	t.Logf("Curren time[%v].....", curtime)
	for i := 0; i < 500; i++ {
		func(cur int) {
			options := []interface{}{
				150, // limit
				cur, // cur time
				1,   // window
				2,   // expire
			}
			val, err := EvalSha(context.TODO(), client, sha, []string{"test_yy_test22222"}, options...)
			if err != nil {
				t.Errorf("EvalSha fail, script[%s] err[%+v]", luaScriptMap["FixedWindowScript"], err)
			} else {
				t.Logf("Run succ, time[%v] ret[%v]", cur, val)
			}
		}(int(curtime) + i)
	}
}

// go test . -v -run=TestLimiter_RedisKeyRandomSuffix
func TestLimiter_RedisKeyRandomSuffix(t *testing.T) {
	//
	limits := []int64{1, 3999, 4999, 5000, 5001, 7000, 9999, 10000, 10001, 12500, 14999}
	for _, limit := range limits {
		t.Logf("LimitCount value[%v]", limit)
		for i := 0; i < 10; i++ {
			obj := NewRateLimiter("test", FixedWindowType)
			obj1 := obj.WithOption(Options{
				LimitCount: limit,    // 限流大小
				TimeRange:  int64(2), // 窗口大小
			})
			key1 := obj1.GetRedisKey()
			t.Logf("default index[%v] key[%v]", i, key1)
		}
	}
}

// go test . -v -run=TestLimiter_SetRedisKey
func TestLimiter_SetRedisKey(t *testing.T) {
	obj := NewRateLimiter("test", FixedWindowType)
	obj1 := obj.WithOption(Options{
		LimitCount: int64(5), // 限流大小
		TimeRange:  int64(2), // 窗口大小
	})
	key1 := obj1.GetRedisKey()
	ret1, err1 := obj1.Do()
	t.Logf("default key[%v] ret1[%v] err1[%v]", key1, ret1, err1)

	obj2 := obj.WithOption(Options{
		LimitCount: int64(5), // 限流大小
		TimeRange:  int64(2), // 窗口大小
	}).SetRedisKey("test_test_123456789")
	key2 := obj2.GetRedisKey()
	ret2, err2 := obj2.Do()
	t.Logf("custom key[%v] ret2[%v] err2[%v]", key2, ret2, err2)
}

// 令牌桶 - 根据时间顺序有序发放
//
// 根据下面命令可得出: 每次令牌发放间隔相同
// go test . -v -run=TestLimiter_TokenBucketLimiter_1 | grep 'Run succ' | grep -v 'ret\[0\]'
func TestLimiter_TokenBucketLimiter_1(t *testing.T) {
	sha, err := LoadScript(context.TODO(), client, luaScriptMap["TokenBucketScript"])
	if err != nil {
		t.Errorf("LoadScript fail, script[%s] err[%+v]", luaScriptMap["TokenBucketScript"], err)
	}

	curtime := time.Now().UnixMilli()
	t.Logf("Curren time[%v].....", curtime)
	for i := 0; i < 600; i++ {
		func(cur int) {
			options := []interface{}{
				200,  // intervalPerPermit
				cur,  // curtime, ms
				5,    // bucketMaxTokens
				1000, // resetBucketInterval, ms
				0,    // initTokens
			}
			val, err := EvalSha(context.TODO(), client, sha, []string{"test_yy_test333333"}, options...)
			if err != nil {
				t.Errorf("EvalSha fail, script[%s] err[%+v]", luaScriptMap["TokenBucketScript"], err)
			} else {
				t.Logf("Run succ, time[%v] ret[%v]", cur, val)
			}
		}(int(curtime) + i)
	}
}

// 令牌桶 - 与初始设置时间相同的操作（需要有初始Token）
//
// 根据下面命令可得出: 令牌初始化时间重复请求会自减令牌数，直至全部消耗掉
// go test . -v -run=TestLimiter_TokenBucketLimiter_2 | grep 'Run succ' | grep -v 'ret\[0\]'
func TestLimiter_TokenBucketLimiter_2(t *testing.T) {
	sha, err := LoadScript(context.TODO(), client, luaScriptMap["TokenBucketScript"])
	if err != nil {
		t.Errorf("LoadScript fail, script[%s] err[%+v]", luaScriptMap["TokenBucketScript"], err)
	}

	curtime := time.Now().UnixMilli()
	t.Logf("Curren time[%v].....", curtime)
	for i := 0; i < 10; i++ {
		func(cur int) {
			options := []interface{}{
				200,  // intervalPerPermit
				cur,  // curtime, ms
				5,    // bucketMaxTokens
				1000, // resetBucketInterval, ms
				5,    // initTokens
			}
			val, err := EvalSha(context.TODO(), client, sha, []string{"test_yy_test333333"}, options...)
			if err != nil {
				t.Errorf("EvalSha fail, script[%s] err[%+v]", luaScriptMap["TokenBucketScript"], err)
			} else {
				t.Logf("Run succ, time[%v] ret[%v]", cur, val)
			}
		}(int(curtime))
	}
}

// 令牌桶 - 前后两个令牌生成周期内的操作过程
//
// 根据下面命令可得出: 令牌周期内仅发放一次令牌
// go test . -v -run=TestLimiter_TokenBucketLimiter_3 | grep 'Run succ'
func TestLimiter_TokenBucketLimiter_3(t *testing.T) {
	sha, err := LoadScript(context.TODO(), client, luaScriptMap["TokenBucketScript"])
	if err != nil {
		t.Errorf("LoadScript fail, script[%s] err[%+v]", luaScriptMap["TokenBucketScript"], err)
	}

	curtime := time.Now().UnixMilli()
	t.Logf("Curren time[%v].....", curtime)
	for i := 0; i < 31; i++ {
		func(cur int) {
			options := []interface{}{
				10,   // intervalPerPermit
				cur,  // curtime, ms
				100,  // bucketMaxTokens
				1000, // resetBucketInterval, ms
				0,    // initTokens
			}
			val, err := EvalSha(context.TODO(), client, sha, []string{"test_yy_test333333"}, options...)
			if err != nil {
				t.Errorf("EvalSha fail, script[%s] err[%+v]", luaScriptMap["TokenBucketScript"], err)
			} else {
				t.Logf("Run succ, time[%v] ret[%v]", cur, val)
			}
		}(int(curtime) + i)
	}
}

// 令牌桶 - 某时间点执行重复操作
//
// 根据下面命令可得出: 无论初始化还是之后的某个时间点，只要重复请求均会返回令牌（存在令牌的情况）
// go test . -v -run=TestLimiter_TokenBucketLimiter_4 | grep 'Run succ' | grep -v 'ret\[0\]'
func TestLimiter_TokenBucketLimiter_4(t *testing.T) {
	sha, err := LoadScript(context.TODO(), client, luaScriptMap["TokenBucketScript"])
	if err != nil {
		t.Errorf("LoadScript fail, script[%s] err[%+v]", luaScriptMap["TokenBucketScript"], err)
	}

	curtime := time.Now().UnixMilli()
	t.Logf("Curren time[%v].....", curtime)
	for i := 0; i < 150; i++ {
		tmp := int(curtime)
		if i < 5 {
			tmp = int(curtime)
		} else if i > 10 && i < 15 {
			tmp = int(curtime) + 5
		} else {
			tmp += i
		}

		func(cur int) {
			options := []interface{}{
				10,   // intervalPerPermit
				cur,  // curtime, ms
				8,    // bucketMaxTokens
				1000, // resetBucketInterval, ms
				0,    // initTokens
			}
			val, err := EvalSha(context.TODO(), client, sha, []string{"test_yy_test333333"}, options...)
			if err != nil {
				t.Errorf("EvalSha fail, script[%s] err[%+v]", luaScriptMap["TokenBucketScript"], err)
			} else {
				t.Logf("Run succ, time[%v] ret[%v]", cur, val)
			}
		}(tmp)
	}
}

// 令牌桶 - 跨令牌桶生成时间间隔场景
//
// 根据下面命令可得出: 当下次访问时间与前一次访问时间多余2两个令牌时，会发放两个令牌，第一个令牌在预期时间使用，第二个则在当前时间或下一毫秒内使用
// go test . -v -run=TestLimiter_TokenBucketLimiter_5 | grep 'Run succ' | grep -v 'ret\[0\]'
func TestLimiter_TokenBucketLimiter_5(t *testing.T) {
	sha, err := LoadScript(context.TODO(), client, luaScriptMap["TokenBucketScript"])
	if err != nil {
		t.Errorf("LoadScript fail, script[%s] err[%+v]", luaScriptMap["TokenBucketScript"], err)
	}

	curtime := time.Now().UnixMilli()
	t.Logf("Curren time[%v].....", curtime)
	for i := 0; i < 150; i++ {
		tmp := int(curtime)
		tmp += i
		if i >= 60 {
			tmp += 50
		} else if i >= 30 {
			tmp += 20
		}

		func(cur int) {
			options := []interface{}{
				10,   // intervalPerPermit
				cur,  // curtime, ms
				8,    // bucketMaxTokens
				1000, // resetBucketInterval, ms
				1,    // initTokens
			}
			val, err := EvalSha(context.TODO(), client, sha, []string{"test_yy_test333333"}, options...)
			if err != nil {
				t.Errorf("EvalSha fail, script[%s] err[%+v]", luaScriptMap["TokenBucketScript"], err)
			} else {
				t.Logf("Run succ, time[%v] ret[%v]", cur, val)
			}
		}(tmp)
	}
}

// 令牌桶 - 初始化令牌数不为0
//
// 根据下面命令可得出: 初始令牌数不为0时，请求会在同一时刻被消费掉，当前请求消费掉之后再执行令牌匀速生成消费逻辑
// go test . -v -run=TestLimiter_TokenBucketLimiter_6 | grep 'Run succ' | grep -v 'ret\[0\]'
func TestLimiter_TokenBucketLimiter_6(t *testing.T) {
	sha, err := LoadScript(context.TODO(), client, luaScriptMap["TokenBucketScript"])
	if err != nil {
		t.Errorf("LoadScript fail, script[%s] err[%+v]", luaScriptMap["TokenBucketScript"], err)
	}

	curtime := time.Now().Unix() * 1000
	t.Logf("Curren time[%v].....", curtime)
	for i := 0; i < 220; i++ {
		tmp := int(curtime) + i*100

		func(cur int) {
			options := []interface{}{
				10000, // intervalPerPermit
				cur,   // curtime, ms
				2,     // bucketMaxTokens
				20000, // resetBucketInterval, ms
				5,     // initTokens
			}
			val, err := EvalSha(context.TODO(), client, sha, []string{"test_yy_test44444"}, options...)
			if err != nil {
				t.Errorf("EvalSha fail, script[%s] err[%+v]", luaScriptMap["TokenBucketScript"], err)
			} else {
				t.Logf("Run succ, time[%v] ret[%v]", cur, val)
			}
		}(tmp)
	}
}

func TestLimiter_LeakyBucketLimiter(t *testing.T) {
	sha, err := LoadScript(context.TODO(), client, luaScriptMap["LeakyBucketScript"])
	if err != nil {
		fmt.Printf("LoadScript fail, script[%s] err[%+v]\n", luaScriptMap["LeakyBucketScript"], err)
	}

	curtime := time.Now().UnixMilli()
	for i := 0; i < 100; i++ {
		func(cur int) {
			options := []interface{}{
				2,          // capacity   桶的容量
				1,          // limitCount 漏水速率
				cur / 1000, // 当前时间
			}
			val, err := EvalSha(context.TODO(), client, sha, []string{"test_yy_test"}, options...)
			fmt.Printf("Run succ, index[%v] time[%v] ret[%v] err[%v]\n", i, cur/1000, val, err)
		}(int(curtime) + i*100)
	}
}

func TestLeakyBucket(t *testing.T) {
	tests := []struct {
		name     string
		capacity int
		leakRate int
		requests int
		interval time.Duration
		want     int
	}{
		{
			name:     "正常限流",
			capacity: 5,
			leakRate: 1,
			requests: 10,
			interval: time.Second,
			want:     5,
		},
		{
			name:     "快速漏水",
			capacity: 3,
			leakRate: 5,
			requests: 10,
			interval: time.Millisecond * 100,
			want:     3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := NewRateLimiter("test", LeakyBucketType)
			passed := 0

			for i := 0; i < tt.requests; i++ {
				rr, err := obj.WithOption(Options{
					Capacity:   int64(tt.capacity),
					LimitCount: int64(tt.leakRate),
				}).Do()

				t.Logf("result: %v, err: %v", rr, err)
				if rr > 0 {
					passed++
				}
				time.Sleep(tt.interval)
			}

			if passed > tt.want {
				t.Errorf("通过请求数 %d 超过预期 %d", passed, tt.want)
			}
		})
	}
}

func BenchmarkLimiter_FixedWindowLimiter(b *testing.B) {
	sha, err := LoadScript(context.TODO(), client, luaScriptMap["FixedWindowScript"])
	if err != nil {
		fmt.Printf("LoadScript fail, script[%s] err[%+v]\n", luaScriptMap["FixedWindowScript"], err)
	}

	for i := 0; i < b.N; i++ {
		options := []interface{}{
			30,
			1,
			2,
		}
		val, _ := EvalSha(context.TODO(), client, sha, []string{"test_yy_test1111111"}, options...)
		fmt.Printf("Run succ, index[%v] ret[%v]\n", i, val)
	}
}

func BenchmarkLimiter_SlideWindowLimiter(b *testing.B) {
	sha, err := LoadScript(context.TODO(), client, luaScriptMap["FixedWindowScript"])
	if err != nil {
		fmt.Printf("LoadScript fail, script[%s] err[%+v]\n", luaScriptMap["SlideWindowScript"], err)
	}

	curtime := time.Now().UnixMilli()
	for i := 0; i < b.N; i++ {
		func(cur int) {
			options := []interface{}{
				1000, // limit
				cur,  // curtime
				1,    // window
				2,    // expire
			}
			val, _ := EvalSha(context.TODO(), client, sha, []string{"test_yy_test222222"}, options...)
			fmt.Printf("Run succ, index[%v] ret[%v]\n", i, val)
		}(int(curtime) + i)
	}
}
