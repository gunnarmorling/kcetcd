# kc-etcd

_An example Kafka Connect source connector, ingesting changes from etcd_

The goal of this project is not primarily to provide a production-ready connector for [etcd](https://etcd.io/),
but rather to serve as an example for a complete yet simple Kafka Connect source connector,
adhering to best practices -- such as supporting multiple tasks --
and serving as an example connector for learning purposes
(e.g. on connector implementation and testing) and basis for explorations of related KIPs such as [KIP-618](https://cwiki.apache.org/confluence/display/KAFKA/KIP-618%3A+Exactly-Once+Support+for+Source+Connectors)
("Exactly-Once Support for Source Connectors").

## Usage

```shell
http PUT localhost:8083/connectors/test-connector/config < register-test.json
```

## Build

This project requires OpenJDK 11 or later for its build.
Apache Maven is used for the build.
Run the following to build the project:

```shell
mvn clean verify
```

## Testing With Docker Compose

The Docker Compose set-up in _docker-compose.yml_ provides the following infrastructure for manual testing:

* Apache Kafka and ZooKeeper
* Kafka Connect with the etcd connector
* Three etcd clusters: one with three nodes, two with one nodes each

Prepare the connector plug-in:

```shell
mvn clean verify -Pstage
```

Start Apache Kafka, Kafka Connect, ZooKeeper, and etcd:

```shell
docker-compose up
```

Register the connector:

```shell
http PUT localhost:8083/connectors/test-connector/config < register-test.json
```

Put something into one of the etcd clusters:

```shell
docker-compose exec etcd-a-1 /bin/sh -c "ETCDCTL_API=3 /usr/local/bin/etcdctl put foo bar"
```

Consume events from Kafka:

```shell
docker-compose exec kafka /kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 \
    --from-beginning \
    --property print.key=true \
    --topic etcd-a
```

Apply equivalent steps for clusters/topics _etcd-b_ and _etcd-c_.

Shut down:

```shell
docker-compose down
```

### Useful Commands

Delete connector:

```shell
http DELETE http://localhost:8083/connectors/test-connector
```

List topics:

```shell
docker-compose exec kafka /kafka/bin/kafka-topics.sh --zookeeper zookeeper:2181 --list
```

## License

This code base is available under the Apache License, version 2.
