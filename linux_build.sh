



docker run --rm -i -v `pwd`:/go/src/ci -w /go/src/ci  dev_centos:v1.2   go build -o main  main.go
