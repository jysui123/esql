#!/bin/bash

go build
golint *.go
go test