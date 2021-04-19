# kc-etcd

An example Kafka Connect source connector, ingesting changes from etcd.

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

Prepare the connector plug-in:

```shell
mvn clean verify -Pstage
```

Start Apache Kafka, Kafka Connect, ZooKeeper, and etcd:

```shell
docker-compose up
```

Register connector:

```shell
http PUT localhost:8083/connectors/test-connector/config < register-test.json
```

Put something into etcd:

```shell
docker-compose exec etcd /bin/sh -c "ETCDCTL_API=3 /usr/local/bin/etcdctl put foo bar
```

Consume events from Kafka:

```shell
docker-compose exec kafka /kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 \
    --from-beginning \
    --property print.key=true \
    --topic etcd-1
```

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
