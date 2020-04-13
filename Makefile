TARGETS="freebsd/amd64 linux/amd64"
COMMIT_HASH=`git rev-parse --short HEAD 2>/dev/null`

check: lint test test-race

fmt:
	@for d in $(DIRS) ; do \
		if [ "`gofmt -s -w $$d/*.go | tee /dev/stderr`" ]; then \
			echo "^ error formatting go files" && echo && exit 1; \
		fi \
	done

lint:
	@if [ "`golangci-lint run | tee /dev/stderr`" ]; then \
		echo "^ golangci-lint errors!" && echo && exit 1; \
	fi

get:
	go get -v -d -t ./...

test:
	go test ./...

test-race:
	go test -race ./...

build:
	${GOPATH}/bin/gox -ldflags="-w -s" -osarch=${TARGETS}

build-dev:
	${GOPATH}/bin/gox -osarch=${TARGETS} -output="{{.Dir}}_{{.OS}}_{{.Arch}}-${COMMIT_HASH}"
