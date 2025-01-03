// Copyright(C) 2024 Github Inc. All Rights Reserved.
// Author: metrue8@gmail.com
// Date:   2024/01/03

package ratelimiter

import (
	"context"
	"sync"

	"github.com/redis/go-redis/v9"
)

// ScriptShas 定义存储Sha值全局变量
var ScriptShas *ScriptSha

// redisClient 存储 Redis 资源实例
var redisClient *redis.Client

// compressFlag 定义是否启用折叠代码标记
var compressFlag bool

// ScriptSha 定义存储Load脚本后的Sha值结构体
type ScriptSha struct {
	FixedWindow string
	SlideWindow string
	TokenBucket string
	LeakyBucket string
}

// Init  初始化配置
func Init(client *redis.Client, compress bool) {
	// 设置Redis实例
	redisClient = client

	// 设置折叠代码标记
	compressFlag = compress

	// 启动时加载Lua脚本
	loadRedisScript(client)
}

// loadRedisScript 预加载Lua脚本
func loadRedisScript(client *redis.Client) {
	var onece sync.Once
	onece.Do(func() {
		ctx := context.TODO()
		ScriptShas = &ScriptSha{}
		if res, err := LoadScript(ctx, client, getLuaScript(FixedWindowType, compressFlag)); err == nil {
			ScriptShas.FixedWindow = res
		}
		if res, err := LoadScript(ctx, client, getLuaScript(SlideWindowType, compressFlag)); err == nil {
			ScriptShas.SlideWindow = res
		}
		if res, err := LoadScript(ctx, client, getLuaScript(TokenBucketType, compressFlag)); err == nil {
			ScriptShas.TokenBucket = res
		}
		if res, err := LoadScript(ctx, client, getLuaScript(LeakyBucketType, compressFlag)); err == nil {
			ScriptShas.LeakyBucket = res
		}
	})
}
