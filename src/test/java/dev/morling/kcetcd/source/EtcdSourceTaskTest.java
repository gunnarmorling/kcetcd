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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import dev.morling.kcute.TaskRunner;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.etcd.jetcd.test.EtcdClusterExtension;

import static org.assertj.core.api.Assertions.assertThat;

public class EtcdSourceTaskTest {

    @RegisterExtension
    public static final EtcdCluster etcd = new EtcdClusterExtension("test-etcd", 1);

    @RegisterExtension
    public TaskRunner taskRunner = TaskRunner.forSourceTask(EtcdSourceConnectorTask.class)
            .with("endpoints", "test-etcd#" + etcd.getClientEndpoints().get(0))
            .build();

    @Test
    public void shouldHandleAllTypesOfEvents() throws Exception {
        Client client = Client.builder()
                .endpoints(etcd.getClientEndpoints()).build();

        KV kvClient = client.getKVClient();
        long currentRevision = getCurrentRevision(kvClient);

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

        List<SourceRecord> records = taskRunner.take("test-etcd", 4);

        // insert 1
        SourceRecord record = records.get(0);
        assertThat(record.sourcePartition()).isEqualTo(Collections.singletonMap("name", "test-etcd"));
        assertThat(record.sourceOffset()).isEqualTo(Collections.singletonMap("revision", ++currentRevision));
        assertThat(record.keySchema()).isEqualTo(Schema.STRING_SCHEMA);
        assertThat(record.key()).isEqualTo("key-1");
        assertThat(record.valueSchema()).isEqualTo(Schema.STRING_SCHEMA);
        assertThat(record.value()).isEqualTo("value-1");

        // insert 2
        record = records.get(1);
        assertThat(record.sourceOffset()).isEqualTo(Collections.singletonMap("revision", ++currentRevision));
        assertThat(record.key()).isEqualTo("key-2");
        assertThat(record.value()).isEqualTo("value-2");

        // update
        record = records.get(2);
        assertThat(record.sourceOffset()).isEqualTo(Collections.singletonMap("revision", ++currentRevision));
        assertThat(record.key()).isEqualTo("key-1");
        assertThat(record.value()).isEqualTo("value-1a");

        // delete
        record = records.get(3);
        assertThat(record.sourceOffset()).isEqualTo(Collections.singletonMap("revision", ++currentRevision));
        assertThat(record.key()).isEqualTo("key-2");
        assertThat(record.value()).isNull();
    }

    private long getCurrentRevision(KV kvClient) throws InterruptedException, ExecutionException {
        return kvClient.get(ByteSequence.from("not-existent".getBytes()))
                .get()
                .getHeader()
                .getRevision();
    }

    @Test
    public void shouldCatchUpWithChangesAfterRestart() throws Exception {
        Client client = Client.builder()
                .endpoints(etcd.getClientEndpoints()).build();

        KV kvClient = client.getKVClient();
        long currentRevision = getCurrentRevision(kvClient);

        // insert 1
        ByteSequence key = ByteSequence.from("key-1".getBytes());
        ByteSequence value = ByteSequence.from("value-1".getBytes());
        kvClient.put(key, value).get();

        List<SourceRecord> records = taskRunner.take("test-etcd", 1);

        // Asserting one record before stopping, so to make sure an offset has been written
        // insert 1
        SourceRecord record = records.get(0);
        assertThat(record.sourceOffset()).isEqualTo(Collections.singletonMap("revision", ++currentRevision));
        assertThat(record.key()).isEqualTo("key-1");
        assertThat(record.value()).isEqualTo("value-1");

        // insert 2
        key = ByteSequence.from("key-2".getBytes());
        value = ByteSequence.from("value-2".getBytes());
        kvClient.put(key, value).get();

        taskRunner.stop();

        // insert 3
        key = ByteSequence.from("key-3".getBytes());
        value = ByteSequence.from("value-3".getBytes());
        kvClient.put(key, value).get();

        // update 1
        key = ByteSequence.from("key-2".getBytes());
        value = ByteSequence.from("value-2a".getBytes());
        kvClient.put(key, value).get();

        // delete
        key = ByteSequence.from("key-2".getBytes());
        kvClient.delete(key).get();

        taskRunner.start();

        // insert 4
        key = ByteSequence.from("key-3".getBytes());
        value = ByteSequence.from("value-3".getBytes());
        kvClient.put(key, value).get();

        // update 2
        key = ByteSequence.from("key-2".getBytes());
        value = ByteSequence.from("value-2b".getBytes());
        kvClient.put(key, value).get();

        records = taskRunner.take("test-etcd", 6);

        // insert 2
        record = records.get(0);
        assertThat(record.sourceOffset()).isEqualTo(Collections.singletonMap("revision", ++currentRevision));
        assertThat(record.key()).isEqualTo("key-2");
        assertThat(record.value()).isEqualTo("value-2");

        // insert 3
        record = records.get(1);
        assertThat(record.sourceOffset()).isEqualTo(Collections.singletonMap("revision", ++currentRevision));
        assertThat(record.key()).isEqualTo("key-3");
        assertThat(record.value()).isEqualTo("value-3");

        // update 1
        record = records.get(2);
        assertThat(record.sourceOffset()).isEqualTo(Collections.singletonMap("revision", ++currentRevision));
        assertThat(record.key()).isEqualTo("key-2");
        assertThat(record.value()).isEqualTo("value-2a");

        // delete
        record = records.get(3);
        assertThat(record.sourceOffset()).isEqualTo(Collections.singletonMap("revision", ++currentRevision));
        assertThat(record.key()).isEqualTo("key-2");
        assertThat(record.value()).isNull();

        // insert 4
        record = records.get(4);
        assertThat(record.sourceOffset()).isEqualTo(Collections.singletonMap("revision", ++currentRevision));
        assertThat(record.key()).isEqualTo("key-3");
        assertThat(record.value()).isEqualTo("value-3");

        // update 2
        record = records.get(5);
        assertThat(record.sourceOffset()).isEqualTo(Collections.singletonMap("revision", ++currentRevision));
        assertThat(record.key()).isEqualTo("key-2");
        assertThat(record.value()).isEqualTo("value-2b");
    }
}
