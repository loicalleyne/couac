couac 🦆🛢️ 
===================
[![Go Reference](https://pkg.go.dev/badge/github.com/loicalleyne/couac.svg)](https://pkg.go.dev/github.com/loicalleyne/couac)

Go library that provides a helpful wrapper around ADBC for DuckDB.

## Features 
- Bulk inserts into DuckDB from an Arrow record

## Planned
- Support for other statement operations supported in ADBC

## 🚀 Install

Using couac is easy. First, use `go get` to install the latest version
of the library.

```sh
go get -u github.com/loicalleyne/couac@latest
```

## 💡 Usage

You can import `couac` using:

```go
import "github.com/loicalleyne/couac"
...
quack, _ := NewQuack("duck.db")
defer quack.Close()
_, err := couac.IngestCreateAppend(ctx, "destination_table", arrowRecord)
```

## 💫 Show your support

Give a ⭐️ if this project helped you!
Feedback and PRs welcome.

## License

Couac is released under the Apache 2.0 license. See [LICENCE.txt](LICENCE.txt)