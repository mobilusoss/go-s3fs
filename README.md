go-s3fs
====

[![Build Status](https://cloud.drone.io/api/badges/mobilusoss/go-s3fs/status.svg)](https://cloud.drone.io/mobilusoss/go-s3fs)
[![codecov](https://codecov.io/gh/mobilusoss/go-s3fs/branch/master/graph/badge.svg)](https://codecov.io/gh/mobilusoss/go-s3fs)
[![Go Report Card](https://goreportcard.com/badge/github.com/mobilusoss/go-s3fs)](https://goreportcard.com/report/github.com/mobilusoss/go-s3fs)
[![codebeat badge](https://codebeat.co/badges/e03e3de0-9d71-43ce-a6ac-8e0c6445485a)](https://codebeat.co/projects/github-com-mobilusoss-go-s3fs-master)
![GitHub](https://img.shields.io/github/license/mobilusoss/go-s3fs.svg)

Amazon S3 wrapper for human

<!-- toc -->

## Overview

AWS offers a complex and bizarre SDK. We needed a library that would make it easier and faster to use S3.  
go-s3fs solves this problem. It wraps the official SDK, making S3 extremely easy to use.

## Installation

```bash
go get -u github.com/mobilusoss/go-s3fs
```

## Usage

### Connect to S3 using IAM role

```go
package main

import (
	"fmt"
	"github.com/mobilusoss/go-s3fs"
)

func main() {
	fs := s3fs.New(&s3fs.Config{
		Bucket: "samplebucket",
	})
	readCloser, err := fs.Get("/file.txt")
	if err != nil {
		panic("s3 error")
	}
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(*readCloser); err != nil {
		panic("io error")
	}
	text := buf.String()
	fmt.Println(text)
}
```

### Connect to [MinIO](https://github.com/minio/minio)

```go
package main

import (
	"fmt"
	"github.com/mobilusoss/go-s3fs"
)

func main() {
	fs := s3fs.New(&s3fs.Config{
		Bucket: "samplebucket",
		EnableMinioCompat: true,
		Endpoint: "http://127.0.0.1:9000",
		EnableIAMAuth: true,
		AccessKeyID: "accesskey",
		AccessSecretKey: "secretkey",
	})
	readCloser, err := fs.Get("/file.txt")
	if err != nil {
		panic("s3 error")
	}
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(*readCloser); err != nil {
		panic("io error")
	}
	text := buf.String()
	fmt.Println(text)
}
```

### Can be logically divided tenant

```go
package main

import (
	"github.com/mobilusoss/go-s3fs"
)

func main() {
	_ = s3fs.New(&s3fs.Config{
		Bucket: "samplebucket",
		Domain: "tenantone",
	})
}
```

### Can be logically divided tenant per application

```go
package main

import (
	"github.com/mobilusoss/go-s3fs"
)

func main() {
	_ = s3fs.New(&s3fs.Config{
		Bucket: "samplebucket",
		Namespace: "appone",
		Domain: "tenantone",
	})
}
```

## License

MIT