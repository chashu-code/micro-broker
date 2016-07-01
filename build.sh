#!/usr/bin/env bash

cd `dirname $0`
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./release/micro-broker-linux
CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o ./release/micro-broker-osx
