SHELL        := bash
.SHELLFLAGS  := -Eeuo pipefail -c

include Makefile.versions.mk

PGQUE_SQL_URL := https://raw.githubusercontent.com/NikolayS/pgque/$(PGQUE_COMMIT)/sql/pgque.sql
PGQUE_SQL     := e2e/testdata/pgque.sql

.PHONY: fetch-schema
fetch-schema: $(PGQUE_SQL) ## Download pgque.sql at the pinned commit

$(PGQUE_SQL):
	curl -sSfL $(PGQUE_SQL_URL) -o $@

.PHONY: format
format: ## Format all Go code with golangci-lint fmt
	go tool golangci-lint fmt ./...
	go tool golangci-lint fmt ./e2e/...

.PHONY: lint
lint: ## Run golangci-lint on all modules
	go tool golangci-lint run ./...
	go tool golangci-lint run ./e2e/...

.PHONY: test
test: ## Run unit tests
	go test -race ./...

.PHONY: test.e2e
test.e2e: fetch-schema ## Run e2e tests (downloads schema if missing)
	go test -race -count=1 -timeout 120s ./e2e/...

.PHONY: help
help:
	@grep -E '^[a-zA-Z_.-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "%-20s %s\n", $$1, $$2}'
