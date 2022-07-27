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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;

import static org.assertj.core.api.Assertions.assertThat;

public class EventTypesSourceTaskTest extends EtcdSourceTaskTestBase {

    @Test
    public void shouldHandleAllTypesOfEvents() throws Exception {
        Client client = Client.builder()
                .endpoints(etcd.clientEndpoints()).build();

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
}
