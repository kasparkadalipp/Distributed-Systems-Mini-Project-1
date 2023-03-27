# Distributed tic-tac-toe

This project was implemented as part of the distributed systems course by following authors:
- Kaspar Kadalipp
- Daniel Würsch
- Joshua Katigbak

## Requirements

Required applications:
* [etcd](https://etcd.io/)

Required python libraries:
* [etcd3](https://pypi.org/project/etcd3/)
* protobuf
* grpcio

## Running

### Generating protocol classes

GRPC protocol classes can be generated using following command:

```
python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. protocol.proto
```

### etcd

`etcd` needs to be available for nodes and should be started. In case nodes are remote, `etcd` needs to be started to allow remote connections:

```
etcd --listen-client-urls 'http://0.0.0.0:2379' --advertise-client-urls 'http://0.0.0.0:2379'
```

### Nodes

Nodes can be started with following command:

```
python tictactoe.py port [etcd_host:etcd_port]
```

`etcd` host and port can be omitted in which case `localhost:2379` will be used.

Note, there is no limitation for the number of nodes which are connecting, the leader can support multiple concurrent ongoing games.

## Design decisions

### Assumptions

* No authentication has been implemented, it's assumed all nodes are trusted entities and won't misbehave.
* Nodes may connect/disconnect at any given time, and as long as leader node remains no unrelated game will be impacted.

### Service discovery

To avoid hardcoding the nodes and allow dynamically adding and removing of nodes, `etcd` is used for service discovery purposes.

Each node registers itself periodically with `etcd` and announces its address and port where it is listening for incoming request.

In order to avoid conflicting node ids, we use an atomically increasing counter from etcd to assign the node ids.

### Leader election

Leader election is performed automatically as background task using bullying algorithm.

Lowest process id will be elected as the leader. This is to avoid frequent changes of leaders in case new nodes are joining the cluster and try to assume leader roles. (Because leader node is responsible for the games, and based on task description leader change will result in game state reset)

### Time synchronization

Time synchronization is performed automatically by the leader node as background task. Each nodes time is defined by the system clock and an offset which may be adjusted by the leader.

### Timeout

* Timeout for players can be set on the game node, which checks every 10 seconds and removes stale games with no player actions
* Timeout for the master itself is redundant in our implementation, because faulty master will be detected automatically when nodes query the master every 10 seconds to see if he is healthy. In such a case, reelection of the leader occurs, which automatically invalidates all previous games.
