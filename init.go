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

// ScriptSha 定义存储Load脚本后的Sha值结构体
type ScriptSha struct {
	FixedWindow string
	SlideWindow string
	TokenBucket string
}

// Init  初始化配置
func Init(client *redis.Client) {
	// 设置Redis实例
	redisClient = client

	// 启动时加载Lua脚本
	loadRedisScript(client)
}

// loadRedisScript 预加载Lua脚本
func loadRedisScript(client *redis.Client) {
	var onece sync.Once
	onece.Do(func() {
		ctx := context.TODO()
		ScriptShas = &ScriptSha{}
		if res, err := LoadScript(ctx, client, FixedWindowScript); err == nil {
			ScriptShas.FixedWindow = res
		}

		if res, err := LoadScript(ctx, client, SlideWindowScript); err == nil {
			ScriptShas.SlideWindow = res
		}
		if res, err := LoadScript(ctx, client, TokenBucketScript); err == nil {
			ScriptShas.TokenBucket = res
		}
	})
}
