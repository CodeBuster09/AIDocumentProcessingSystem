.PHONY: up down run build test logs reset psql seed-pdf help

help:
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

up: .env ## Start postgres + minio, wait until healthy
	docker compose up -d --wait
	@echo "postgres  localhost:5432"
	@echo "minio api localhost:9000   console http://localhost:9001 (minioadmin/minioadmin)"

.env:
	cp .env.example .env
	@echo "created .env from .env.example"

down: ## Stop containers, keep data
	docker compose down

reset: ## Stop containers AND delete all data
	docker compose down -v

run: ## Run the API (migrations apply on startup)
	go run ./cmd/api

build: ## Compile to bin/api
	go build -o bin/api ./cmd/api

test: ## Run tests
	go test ./...

logs: ## Tail container logs
	docker compose logs -f

psql: ## Open a psql shell
	docker compose exec postgres psql -U docpipe -d docpipe

seed-pdf: ## Generate testdata/sample.pdf to upload
	@mkdir -p testdata
	@printf 'Async processing lets the API return immediately.\n\fMessage queues decouple producers from consumers.\n' > testdata/sample.txt
	@command -v enscript >/dev/null 2>&1 && enscript -q -p - testdata/sample.txt | ps2pdf - testdata/sample.pdf 2>/dev/null \
		|| /System/Library/Printers/Libraries/convert -f testdata/sample.txt -o testdata/sample.pdf 2>/dev/null \
		|| echo "couldn't generate a PDF automatically — drop any .pdf into testdata/"
	@ls -la testdata/ 2>/dev/null
