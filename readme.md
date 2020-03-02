# KV service

A kv service based on Raft Consistency Algorithm

## Usage

### client

``` bash
cd ${GOPATH}/kvservice
go run client/main.go
```

### server

``` bash
cd ${GOPATH}/kvservice
go run server/main.go server/config.go serverIndex
```

### my kv server

- Server 1:
  
  state: <http://fatwaer.store/kv/state.html>

  log  : <http://fatwaer.store/kv/log.html>

- Server 2:
  
  state: <http://106.13.211.207/kv/state.html>

  log  : <http://106.13.211.207/kv/log.html>

- Server 3:

  state: <http://18.162.39.157/kv/state.html>

  log  : <http://18.162.39.157/kv/log.html>
