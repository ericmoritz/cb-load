.PHONY: data

all: deps cb-load cb-jepsen

deps:
	go get github.com/couchbaselabs/go-couchbase
	go get github.com/mrb/riakpbc

cb-load:
	go build -o cb-load cb-load.go

cb-jepsen:
	go build -o cb-jepsen cb-jepsen.go

clean:
	rm -f cb-load cb-jepsen

data:
	cat data/oc-jump?/results-read-10k.csv | sed "/elapsed/d" | python bin/crunch_numbers.py data/results-read-10k
	cat data/oc-jump?/results-read-1k.csv | sed "/elapsed/d" | python bin/crunch_numbers.py data/results-read-1k
	cat data/oc-jump?/results-write-10k.csv | sed "/elapsed/d" | python bin/crunch_numbers.py data/results-write-10k
	cat data/oc-jump?/results-write-1k.csv | sed "/elapsed/d" | python bin/crunch_numbers.py data/results-write-1k
