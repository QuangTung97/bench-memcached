.PHONY: build run

build:
	go build -o main

run:
	GOGC=off GOMEMLIMIT=512MiB ./main