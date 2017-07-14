TARGETS="freebsd/amd64 linux/amd64"
COMMIT_HASH=`git rev-parse --short HEAD 2>/dev/null`

check: get fmt vet lint test test-race

fmt:
	@for d in $(DIRS) ; do \
		if [ "`gofmt -l $$d/*.go | tee /dev/stderr`" ]; then \
			echo "^ improperly formatted go files" && echo && exit 1; \
		fi \
	done

lint:
	@if [ "`gometalinter --config=linter.json ./... | tee /dev/stderr`" ]; then \
		echo "^ gometalinter errors!" && echo && exit 1; \
	fi

get:
	go get -v -t ./...

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	@if [ "`go vet ./... | tee /dev/stderr`" ]; then \
		echo "^ go vet errors!" && echo && exit 1; \
	fi

build:
	go get github.com/mitchellh/gox
	${GOPATH}bin/gox -ldflags="-w" -osarch=${TARGETS}

build-dev:
	go get github.com/mitchellh/gox
	${GOPATH}bin/gox -osarch=${TARGETS} -output="{{.Dir}}_{{.OS}}_{{.Arch}}-${COMMIT_HASH}"
