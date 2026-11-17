SHELL := /bin/bash
GO_IMAGE  ?= martinet-go:dev
CPP_IMAGE ?= martinet-cpp:dev
KIND_CLUSTER ?= martinet
BREW := $(shell command -v brew >/dev/null && brew --prefix)

.PHONY: help proto build cpp test test-go test-cpp bench images up down logs e2e chaos \
        tf-local k8s-load k8s-apply k8s-delete fmt lint clean

help: ## show targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n",$$1,$$2}'

proto: ## regenerate Go protobuf/gRPC code
	protoc --proto_path=proto \
	  --go_out=gen --go_opt=module=github.com/90Ismail/martinet/gen \
	  --go-grpc_out=gen --go-grpc_opt=module=github.com/90Ismail/martinet/gen \
	  proto/carnet/v1/carnet.proto proto/martinet/v1/martinet.proto

build: ## build Go binaries into bin/
	go build -o bin/ ./cmd/...

cpp: ## build carnet + pouls (C++)
	cmake -S cpp -B cpp/build -DCMAKE_PREFIX_PATH=$(BREW) >/dev/null
	cmake --build cpp/build -j

test: test-go test-cpp ## run all unit tests

test-go:
	go test -race ./...

test-cpp: cpp
	ctest --test-dir cpp/build --output-on-failure

bench: cpp ## LSM-tree benchmark
	./cpp/build/carnet-bench 200000 100

images: ## build both Docker images
	docker build -f deploy/docker/go.Dockerfile  -t $(GO_IMAGE) .
	docker build -f deploy/docker/cpp.Dockerfile -t $(CPP_IMAGE) .

up: images ## start the full stack with docker compose
	docker compose up -d
	@echo "ingest http://localhost:8080  grpc :9090 | status grpc :9091 | mq grpc :7072"

down: ## stop the stack and delete volumes
	docker compose down -v

logs:
	docker compose logs -f --tail=50

e2e: build ## publish a few events and show what happened
	./scripts/demo.sh

chaos: build ## kill components under load; verify nothing is lost
	./scripts/chaos.sh

tf-local: ## create the kind cluster + operators with Terraform
	cd terraform/envs/local && terraform init -input=false && terraform apply -auto-approve

k8s-load: images ## load images into kind
	kind load docker-image $(GO_IMAGE) $(CPP_IMAGE) --name $(KIND_CLUSTER)

k8s-apply: ## deploy to the current kube context (local overlay)
	kubectl apply -k deploy/k8s/overlays/local

k8s-delete:
	kubectl delete -k deploy/k8s/overlays/local --ignore-not-found

fmt: ## format everything
	gofmt -w cmd internal
	terraform fmt -recursive terraform
	command -v clang-format >/dev/null && find cpp -path cpp/build -prune -o \( -name '*.cc' -o -name '*.h' \) -print | xargs clang-format -i || true

lint:
	go vet ./...
	cd terraform/envs/local && terraform init -backend=false -input=false >/dev/null && terraform validate
	cd terraform/envs/gcp   && terraform init -backend=false -input=false >/dev/null && terraform validate

clean:
	rm -rf bin cpp/build
