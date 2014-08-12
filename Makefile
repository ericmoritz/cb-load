all: deps cb-load

deps:
	go get github.com/couchbaselabs/go-couchbase
	go get github.com/mrb/riakpbc

cb-load:
	go build -o cb-load cb-load.go

clean:
	rm -f cb-load
