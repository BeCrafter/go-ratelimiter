// Copyright(C) 2024 Github Inc. All Rights Reserved.
// Author: metrue8@gmail.com
// Date:   2024/01/03

package ratelimiter

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// NoScriptMsg 定义无脚本执行的信息
const NoScriptMsg string = "NOSCRIPT No matching script. Please use EVAL."

// LoadScript 执行脚本加载
func LoadScript(ctx context.Context, client *redis.Client, script string) (string, error) {
	res, err := client.Do(ctx, "SCRIPT", "LOAD", script).Result()
	if _, ok := res.(string); !ok {
		return "", err
	}
	return res.(string), err
}

// ScriptFlush 清空脚本缓存
func ScriptFlush(ctx context.Context, client *redis.Client) bool {
	res, err := client.Do(ctx, "SCRIPT", "FLUSH").Result()
	if err != nil || res.(string) != "ok" {
		return false
	}

	return true
}

// EvalSha 通过Sha值执行脚本
func EvalSha(ctx context.Context, client *redis.Client, sha1 string, keys []string, args ...interface{}) (interface{}, error) {
	cmdArgs := make([]interface{}, 3+len(keys), 3+len(keys)+len(args))
	cmdArgs[0] = "EVALSHA"
	cmdArgs[1] = sha1
	cmdArgs[2] = len(keys)
	for i, key := range keys {
		cmdArgs[3+i] = key
	}
	cmdArgs = append(cmdArgs, args...)
	res, err := client.Do(ctx, cmdArgs...).Result()
	if err != nil && err.Error() == NoScriptMsg {
		// 缺失脚本时重新异步Load
		go func(client *redis.Client) {
			loadRedisScript(client)
		}(client)
	}
	return res, err
}

// Eval 执行脚本
func Eval(ctx context.Context, client *redis.Client, script string, keys []string, args ...interface{}) (interface{}, error) {
	cmdArgs := make([]interface{}, 3+len(keys), 3+len(keys)+len(args))
	cmdArgs[0] = "EVAL"
	cmdArgs[1] = script
	cmdArgs[2] = len(keys)
	for i, key := range keys {
		cmdArgs[3+i] = key
	}
	cmdArgs = append(cmdArgs, args...)
	return client.Do(ctx, cmdArgs...).Result()
}
