.PHONY: test test-day1 test-day2 test-day3 demo-wordcount

test:
	go test ./...

test-day1:
	go test -tags=day1 ./... -count=1

test-day2:
	go test -tags=day2 ./... -count=1

test-day3:
	go test -tags=day3 ./...  -count=1

demo-wordcount:
	go run ./cmd/kvtool wordcount -in ./testdata/text_small.txt

lsm-kv:
	go run ./cmd/kvtool load -count 10000 -store lsm

count-min-sketch:
	go run ./cmd/kvtool load -count 100000 -zipf 1.1 -report
