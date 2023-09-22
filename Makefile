.PHONY: help

help: ## Help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help

build: ## Build all versions and sign with our GPG key
	@env GOOS=linux GOARCH=amd64 go build
	@chmod +x cs-agent
	@gpg2 -u 0xA8C477EE --output cs-agent.sig --detach-sig cs-agent
	@tar -czvf cs-agent-amd64.tar.gz cs-agent cs-agent.sig
	@mkdir -p pkg
	@rm cs-agent cs-agent.sig
	@cp cs-agent-amd64.tar.gz cs-agent.tar.gz
	@mv cs-agent-amd64.tar.gz pkg/
	@mv cs-agent.tar.gz pkg/
	@env GOOS=linux GOARCH=arm64 go build
	@chmod +x cs-agent
	@gpg2 -u 0xA8C477EE --output cs-agent.sig --detach-sig cs-agent
	@tar -czvf cs-agent-arm64.tar.gz cs-agent cs-agent.sig
	@mkdir -p pkg
	@rm cs-agent cs-agent.sig
	@mv cs-agent-arm64.tar.gz pkg/

build-container: ## Build container
	docker build -t ghcr.io/computestacks/backup-agent:latest .
