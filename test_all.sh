#!/bin/bash

go build
golint *.go
go test
python query_test.py