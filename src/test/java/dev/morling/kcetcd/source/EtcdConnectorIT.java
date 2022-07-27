/*
 *  Copyright 2021 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.kcetcd.source;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import io.debezium.testing.testcontainers.Connector.State;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.launcher.EtcdContainer;

import static org.assertj.core.api.Assertions.assertThat;

public class EtcdConnectorIT {

    private static Network network = Network.newNetwork();

    private static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.0"))
            .withNetwork(network);

    public static DebeziumContainer connectContainer = new DebeziumContainer("debezium/connect-base:1.9.5.Final")
            .withFileSystemBind("target/kcetcd-connector", "/kafka/connect/kcetcd-connector")
            .withNetwork(network)
            .withKafka(kafkaContainer)
            .dependsOn(kafkaContainer);

    public static EtcdContainer etcdContainer = new EtcdContainer("gcr.io/etcd-development/etcd:v3.5.4", "etcd-a", Arrays.asList("etcd-a"))
            .withNetworkAliases("etcd")
            .withNetwork(network);

    @BeforeAll
    public static void startContainers() throws Exception {
        createConnectorJar();

        Startables.deepStart(Stream.of(
                kafkaContainer, etcdContainer, connectContainer))
                .join();
    }

    @Test
    public void shouldHandleAllTypesOfEvents() throws Exception {
        Client client = Client.builder()
                .endpoints(etcdContainer.clientEndpoint()).build();

        ConnectorConfiguration connector = ConnectorConfiguration.create()
                .with("connector.class", "dev.morling.kcetcd.source.EtcdSourceConnector")
                .with("clusters", "test-etcd=http://etcd:2379")
                .with("tasks.max", "2")
                .with("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .with("value.converter", "org.apache.kafka.connect.storage.StringConverter");

        connectContainer.registerConnector("my-connector",
                connector);

        connectContainer.ensureConnectorTaskState("my-connector", 0, State.RUNNING);

        KV kvClient = client.getKVClient();

        // insert 1
        ByteSequence key = ByteSequence.from("key-1".getBytes());
        ByteSequence value = ByteSequence.from("value-1".getBytes());
        kvClient.put(key, value).get();

        // insert 2
        key = ByteSequence.from("key-2".getBytes());
        value = ByteSequence.from("value-2".getBytes());
        kvClient.put(key, value).get();

        // update
        key = ByteSequence.from("key-1".getBytes());
        value = ByteSequence.from("value-1a".getBytes());
        kvClient.put(key, value).get();

        // delete
        key = ByteSequence.from("key-2".getBytes());
        kvClient.delete(key).get();

        List<ConsumerRecord<String, String>> records = drain(getConsumer(kafkaContainer), 4);

        // insert 1
        ConsumerRecord<String, String> record = records.get(0);
        assertThat(record.key()).isEqualTo("key-1");
        assertThat(record.value()).isEqualTo("value-1");

        // insert 2
        record = records.get(1);
        assertThat(record.key()).isEqualTo("key-2");
        assertThat(record.value()).isEqualTo("value-2");

        // update
        record = records.get(2);
        assertThat(record.key()).isEqualTo("key-1");
        assertThat(record.value()).isEqualTo("value-1a");

        // delete
        record = records.get(3);
        assertThat(record.key()).isEqualTo("key-2");
        assertThat(record.value()).isNull();
    }

    private List<ConsumerRecord<String, String>> drain(KafkaConsumer<String, String> consumer,
                                                       int expectedRecordCount) {
        consumer.subscribe(Arrays.asList("test-etcd"));
        List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();

        Unreliables.retryUntilTrue(30, TimeUnit.SECONDS, () -> {
            consumer.poll(Duration.ofMillis(50))
                    .iterator()
                    .forEachRemaining(allRecords::add);

            return allRecords.size() >= expectedRecordCount;
        });

        return allRecords;
    }

    private KafkaConsumer<String, String> getConsumer(KafkaContainer kafkaContainer) {
        return new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(),
                new StringDeserializer());
    }

    private static void createConnectorJar() throws IOException {
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        JarOutputStream target = new JarOutputStream(new FileOutputStream("target/kcetcd-connector/etcd-connector.jar"), manifest);
        add(new File("target/classes"), target);
        target.close();
    }

    private static void add(File source, JarOutputStream target) throws IOException {
        String name = source.getPath().replace("\\", "/").replace("target/classes/", "");
        if (source.isDirectory()) {
            if (!name.endsWith("/")) {
                name += "/";
            }
            JarEntry entry = new JarEntry(name);
            entry.setTime(source.lastModified());
            target.putNextEntry(entry);
            target.closeEntry();
            for (File nestedFile : source.listFiles()) {
                add(nestedFile, target);
            }
        }
        else {
            JarEntry entry = new JarEntry(name);
            entry.setTime(source.lastModified());
            target.putNextEntry(entry);
            try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(source))) {
                byte[] buffer = new byte[1024];
                while (true) {
                    int count = in.read(buffer);
                    if (count == -1)
                        break;
                    target.write(buffer, 0, count);
                }
                target.closeEntry();
            }
        }
    }
}
