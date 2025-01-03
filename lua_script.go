// Copyright(C) 2024 Github Inc. All Rights Reserved.
// Author: metrue8@gmail.com
// Date:   2024/01/03

package ratelimiter

import (
	"bufio"
	"fmt"
	"strings"
)

var luaScriptMap, luaScriptOptMap map[string]string

func init() {
	luaScriptMap = make(map[string]string, 4)
	// 固定窗口限流脚本
	luaScriptMap["FixedWindowScript"] = `
		--[[
			Description: 基于 Reids String 实现, 可指定时间窗口作为限流周期

			1. key        - [V] 限流 key
			2. limit      - [V] 限流大小
			3. timeRange  - [-] 窗口大小, 默认窗口1s
			4. expiration - [-] Key的过期时间, 默认过期2s
		--]]

		local key       = KEYS[1]
		local limit     = tonumber(ARGV[1])
		local timeRange = 1
		if ARGV[2] ~= nil then
			timeRange = tonumber(ARGV[2])
		end

		-- 设定过期周期(300~3600s)
		local expiration  = math.ceil(timeRange * 2)
		if ARGV[3] ~= nil then
			expiration = tonumber(ARGV[3])
		end

		local current = tonumber(redis.call('GET', key) or "0")

		-- 超出限流大小
		if current and current >= limit then 
			return 0
		end

		current = redis.call('INCR', key)
		-- 第一次请求, 则设置过期时间
		if current == 1 then
			redis.call('EXPIRE', key, expiration)
		end

		-- 返回剩余可用请求数
		return limit - current + 1
	`
	// 滑动窗口限流脚本
	luaScriptMap["SlideWindowScript"] = `
		--[[
			Description: 基于 Reids Hash 实现, 最小窗口限制为1s, 最大窗口限制为3600s
						流量会在时间窗口基础上再拆分为更细粒度的窗口进行存储, 流量的放开会随着时间的滚动而逐步放开流量限制

			1. key        - [V] 限流 key
			2. limitCount - [V] 单个时间窗口限制数量
			3. curTime    - [V] 当前时间, 单位ms
			4. timeRange  - [V] 时间窗口范围, 传参单位秒, 默认窗口1秒
			5. expiration - [V] 集合key过期时间, 当key过期时会存在瞬时并发的情况, 因此过期时间不能太短或者改用定时清除
		--]]

		local key         = KEYS[1]
		local limitCount  = tonumber(ARGV[1])
		local curTime     = tonumber(ARGV[2])
		local timeRange   = tonumber(ARGV[3]) * 1000
		local expiration  = tonumber(ARGV[4])
		local newTime     = curTime
		local diffVal     = timeRange
		local constKeyCnt = 1000

		if timeRange > 1000 then
			local littleWin = math.ceil(timeRange / constKeyCnt)
			newTime = math.floor(curTime / littleWin)
			diffVal = math.floor(timeRange / littleWin)
		end

		-- 已访问的次数
		local beforeCount  = 0
		local flatMap      = redis.call('HGETALL', key)
		if table.maxn(flatMap) > 0 then
			for i = 1, #flatMap, 2 do
				local ftime = tonumber(flatMap[i])
				if newTime - ftime < diffVal then
					beforeCount = beforeCount + tonumber(flatMap[i + 1])
				else
					redis.call('HDEL', key, tostring(ftime))
				end
			end
		end

		local result = 0
		if limitCount <= beforeCount then
			return result
		end

		result = limitCount - beforeCount
		redis.call('HINCRBY', key, tostring(newTime), '1')
		redis.call('EXPIRE', key, expiration)

		-- 返回剩余可用请求量，含本次请求
		return result
	`
	// 令牌桶限流脚本
	luaScriptMap["TokenBucketScript"] = `
		--[[
			Description: 基于 Reids Hash 实现

			1. key                 - [V] 令牌桶的 key
			2. intervalPerPermit   - [V] 生成令牌的间隔(ms)
			3. curTime             - [V] 当前时间(ms)
			4. bucketMaxTokens     - [V] 令牌桶的上限
			5. resetBucketInterval - [V] 重置桶内令牌的时间间隔(ms)
			6. initTokens          - [-] 令牌桶初始化的令牌数
			
			7. currentTokens       - 当前桶内令牌数
			8. bucket              - 当前 key 的令牌桶对象
		--]]

		local key                 = KEYS[1]
		local intervalPerPermit   = tonumber(ARGV[1])
		local curTime             = tonumber(ARGV[2])
		local bucketMaxTokens     = tonumber(ARGV[3])
		local resetBucketInterval = tonumber(ARGV[4])

		local initTokens          = 0
		if ARGV[5] ~= nil then
			initTokens = tonumber(ARGV[5])
		end


		local currentTokens       = 0
		local bucket = redis.call('HGETALL', key)

		-- 若当前桶未初始化,先初始化令牌桶
		if table.maxn(bucket) == 0 then
			-- 初始桶内令牌
			currentTokens = initTokens
			-- 设置桶最近的填充时间是当前
			redis.call('HSET', key, 'lastRefillTime', curTime)
			-- 如果当前令牌 == 0 ,更新桶内令牌, 返回 0
			redis.call('HSET', key, 'tokensRemaining', currentTokens)
			-- 初始化令牌桶的过期时间, 设置为间隔的 10 倍
			redis.call('PEXPIRE', key, resetBucketInterval * 10)
			-- 返回令牌数
			return math.max(1, currentTokens)
		end

		-- 上次填充时间
		local lastRefillTime = tonumber(bucket[2])
		-- 剩余的令牌数
		local tokensRemaining = tonumber(bucket[4])

		-- 如果当前时间小于或等于上次更新的时间, 当前令牌数量等于桶内令牌数(幂等性)
		if curTime <= lastRefillTime then
			currentTokens = tokensRemaining
		-- 当前时间大于上次填充时间
		else
			-- 拿到当前时间与上次填充时间的时间间隔
			local intervalSinceLast = curTime - lastRefillTime

			-- 如果当前时间间隔 大于 令牌的生成间隔
			if intervalSinceLast > resetBucketInterval then
				-- 将当前令牌填充满
				currentTokens = initTokens

				-- 更新重新填充时间
				redis.call('HSET', key, 'lastRefillTime', curTime)

			-- 如果当前时间间隔 小于 令牌的生成间隔
			else
				-- 可用的令牌数 = 向下取整数( 上次填充时间与当前时间的时间间隔 / 两个令牌许可之间的时间间隔 )
				local availableTokens = math.floor(intervalSinceLast / intervalPerPermit)

				-- 可授予的令牌 > 0 时
				if availableTokens > 0 then
					-- 生成的令牌 = 上次填充时间与当前时间的时间间隔 % 两个令牌许可之间的时间间隔
					local padMillis = math.fmod(intervalSinceLast, intervalPerPermit)

					-- 将当前令牌桶更新到上一次生成时间
					redis.call('HSET', key, 'lastRefillTime', curTime - padMillis)
				end

				-- 更新当前令牌桶中的令牌数
				currentTokens = math.min(availableTokens + tokensRemaining, bucketMaxTokens)
			end
		end

		local tokensCount = currentTokens
		if (currentTokens > 0) then
			currentTokens = currentTokens - 1
			redis.call('HSET', key, 'tokensRemaining', currentTokens) 
		end

		return tokensCount
	`
	// 漏桶限流脚本
	luaScriptMap["LeakyBucketScript"] = `
		--[[
			Description: 主要逻辑是判断当前请求是否可以被放入桶中，如果可以放入则可以执行本次请求，否则拒绝本次请求 - 基于 Redis Hash 实现
			
			漏桶算法的实现主要分为三个步骤：
				1. 未满加水：通过代码 water += 1 进行不停加水的动作。
				2. 漏桶漏水：通过时间差来计算漏水量
				3. 剩余水量：总水量-漏水量

			注解：
				1. 当请求速率小于漏水速率时，几乎所有请求都会通过
				2. 当请求速率大于漏水速率时，超出桶容量的请求会被拒绝
				3. 即使在突发流量下，也能保证处理速率不超过漏水速率
				4. 桶容量决定了系统能处理的突发流量大小

			场景：
				1. API 调用限制
				2. 消息处理队列
				3. 数据库写入限制

			1. key        - [V] 漏桶 Key
			2. capacity   - [V] 桶的容量
			4. leakRate   - [V] 漏水速率, 单位是每秒漏多少个请求
			4. curTime    - [V] 当前时间, 单位s
		--]]

		local key       = KEYS[1]
		local capacity  = tonumber(ARGV[1])
		local leakRate  = tonumber(ARGV[2])
		local curTime   = tonumber(ARGV[3])

		-- 参数校验
		if not capacity or not leakRate or not curTime then
			return 0
		end

		-- 获取桶中当前水量和上次漏水时间
		local mresult = redis.call('HMGET', key, 'currentWater', 'lastLeakTime')
		local currentWater = mresult[1]
		local lastLeakTime = mresult[2]

		-- 获取桶中水量
		currentWater = tonumber(currentWater) or 0

		-- 获取上次漏水时间
		lastLeakTime = tonumber(lastLeakTime) or curTime

		-- 计算距离上次漏水经过的时间
		local elapsedTime = curTime - tonumber(lastLeakTime)

		-- 漏水操作，更新桶中水量 (时间间隔 * 漏水速率 = 漏水水量)
		local leakedWater = math.floor(elapsedTime * leakRate)

		-- 计算桶中剩余水量, 并保证不小于0 (桶中水量 - 漏水水量 = 桶中剩余水量)
		local newWater = math.max(0, tonumber(currentWater) - leakedWater)

		-- 更新桶中水量和上次漏水时间
		redis.call('HMSET', key, 'currentWater', newWater, 'lastLeakTime', curTime)

		-- 定义返回结果 0 表示不允许, 1 表示允许
		local result = 0

		-- 判断是否允许请求通过
		if newWater < capacity then
			-- 这里是将当前返回的水量加1, 代表桶中水量增加了一个请求的量
			local re = redis.call('HINCRBY', key, 'currentWater', 1)
			result = 1
		end

		return result
	`

	// 将脚本注释去除，并折叠为一行
	luaScriptOptMap = make(map[string]string, len(luaScriptMap))
	for k, v := range luaScriptMap {
		luaScriptOptMap[k] = compressCode(v)
	}
}

func removeComments(code string) string {
	var output strings.Builder
	inMultilineComment := false

	scanner := bufio.NewScanner(strings.NewReader(code))
	for scanner.Scan() {
		line := scanner.Text()
		if inMultilineComment {
			if strings.HasSuffix(line, "]]") {
				inMultilineComment = false
				line = strings.TrimSuffix(line, "]]")
			}
			continue
		}

		parts := strings.Split(line, "--[[")
		if len(parts) > 1 {
			output.WriteString(parts[0])
			output.WriteString(strings.TrimSuffix(parts[1], "]]"))
			inMultilineComment = true
			continue
		}

		parts = strings.SplitN(line, "--", 2)
		if len(parts) > 1 {
			output.WriteString(parts[0])
			continue
		}

		output.WriteString(line)
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading input:", err)
	}

	return output.String()
}

// compressCode compresss the Lua code into a single line
func compressCode(code string) string {
	code = removeComments(code)

	for _, item := range []string{"\n", "\t", "     ", "    ", "   ", "  "} {
		code = strings.ReplaceAll(code, item, " ")
	}

	return strings.TrimSpace(code)
}

// getLuaScript 根据限流类型获取对应的 Lua 脚本
func getLuaScript(limitType LimiterType, flag bool) string {
	var result string

	luaScript := luaScriptMap
	if flag {
		luaScript = luaScriptOptMap
	}

	switch limitType {
	case FixedWindowType:
		result = luaScript["FixedWindowScript"]
	case SlideWindowType:
		result = luaScript["SlideWindowScript"]
	case TokenBucketType:
		result = luaScript["TokenBucketScript"]
	case LeakyBucketType:
		result = luaScript["LeakyBucketScript"]
	}

	return result
}
