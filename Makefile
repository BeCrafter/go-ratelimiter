# 版本号变量
VERSION ?= $(shell git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0")
NEXT_VERSION = $(shell echo $(VERSION) | awk -F. '{ printf "v%d.%d.%d", $$1, $$2, $$3+1 }' | sed 's/vv/v/')

# 打tag相关命令
.PHONY: tag
tag:
	@echo "Current version: $(VERSION)"
	@echo "Next version: $(NEXT_VERSION)"
	# @git tag $(NEXT_VERSION)
	# @git push origin $(NEXT_VERSION)
	# @echo "Successfully created and pushed tag $(NEXT_VERSION)"

.PHONY: tag-delete
tag-delete:
	@echo "Deleting git tag $(VERSION)"
	git tag -d $(VERSION)
	git push origin :refs/tags/$(VERSION)

.PHONY: tag-list
tag-list:
	@echo "Listing all tags"
	git tag -l

# Redis相关命令
.PHONY: redis-start
redis-start:
	@echo "Starting Redis server..."
	docker run --name redis-ratelimiter -p 6379:6379 -d redis:latest

.PHONY: redis-stop
redis-stop:
	@echo "Stopping Redis server..."
	docker stop redis-ratelimiter
	docker rm redis-ratelimiter

.PHONY: redis-cli
redis-cli:
	@echo "Connecting to Redis CLI..."
	docker exec -it redis-ratelimiter redis-cli
