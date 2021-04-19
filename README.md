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

## Useful Commands

docker-compose exec etcd /bin/sh -c "ETCDCTL_API=3 /usr/local/bin/etcdctl put foo bar

docker-compose exec kafka /kafka/bin/kafka-topics.sh --zookeeper zookeeper:2181 --list

docker-compose exec kafka /kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 \
    --from-beginning \
    --property print.key=true \
    --topic etcd-1

## License

This code base is available under the Apache License, version 2.
