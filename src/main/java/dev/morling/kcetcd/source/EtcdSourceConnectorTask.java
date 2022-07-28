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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.etcd.jetcd.Client;
import io.etcd.jetcd.Constants;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchEvent.EventType;

public class EtcdSourceConnectorTask extends SourceTask {

    private static final String NAME = "name";
    private static final String REVISION = "revision";

    private static final Logger LOGGER = LoggerFactory.getLogger(EtcdSourceConnectorTask.class);

    private final BlockingQueue<SourceRecord> queue = new ArrayBlockingQueue<>(2048);

    private OffsetStorageReader storageReader;
    private List<ListenerRegistration> registrations;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void initialize(SourceTaskContext context) {
        super.initialize(context);
        storageReader = context.offsetStorageReader();
    }

    @Override
    public void start(Map<String, String> props) {
        LOGGER.debug("Starting task");
        LOGGER.debug("Task configuration properties: {}", props);

        String[] clusters = props.get(EtcdSourceConnector.CLUSTERS).split(";");
        registrations = new ArrayList<>(clusters.length);
        for (String cluster : clusters) {
            String[] parts = cluster.split("=");

            if (parts.length != 2) {
                throw new IllegalArgumentException("Cluster configuration must be given in the form of <name>=endpoint(,endpoint)*(;<name>=endpoint(,endpoint)*)*");
            }

            String name = parts[0];
            Long lastRevision = getStartingRevision(name);
            registrations.add(new ListenerRegistration(name, parts[1].split(","), lastRevision));
        }
    }

    private Long getStartingRevision(String name) {
        Map<String, Object> lastOffset = storageReader.offset(Collections.singletonMap(NAME, name));
        return lastOffset != null ? ((long) lastOffset.get(REVISION)) + 1 : 0L;
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> values = new ArrayList<>();
        queue.drainTo(values);

        if (values.isEmpty()) {
            Thread.sleep(100);
        }

        return values;
    }

    @Override
    public void stop() {
        LOGGER.debug("Stopping task");
        queue.clear();

        for (ListenerRegistration listenerRegistration : registrations) {
            listenerRegistration.close();
        }
    }

    private class ListenerRegistration {

        private final String name;
        private final Client client;
        private final Watch watch;
        private final Watch.Watcher watcher;

        private ListenerRegistration(String name, String endpoints[], long startingRevision) {
            this.name = name;
            client = Client.builder()
                    .keepaliveWithoutCalls(false)
                    .endpoints(endpoints)
                    .build();

            watch = client.getWatchClient();

            Watch.Listener listener = Watch.listener(response -> {
                for (WatchEvent event : response.getEvents()) {
                    LOGGER.trace("Received event: {}", event.getEventType());
                    queue.offer(toSourceRecord(event));
                }
            });

            watcher = watch.watch(
                    Constants.NULL_KEY,
                    WatchOption.newBuilder().withRange(Constants.NULL_KEY)
                            .withRevision(startingRevision)
                            .build(),
                    listener);
        }

        private SourceRecord toSourceRecord(WatchEvent event) {
            String key = event.getKeyValue().getKey().toString(StandardCharsets.UTF_8);

            String value = null;

            if (event.getEventType() == EventType.PUT) {
                if (event.getKeyValue().getValue() != null) {
                    value = event.getKeyValue().getValue().toString(StandardCharsets.UTF_8);
                }
            }

            return new SourceRecord(
                    Collections.singletonMap(NAME, name),
                    Collections.singletonMap(REVISION, event.getKeyValue().getModRevision()),
                    name,
                    Schema.STRING_SCHEMA,
                    key,
                    value != null ? Schema.STRING_SCHEMA : null,
                    value);
        }

        public void close() {
            watcher.close();
            watch.close();
            client.close();
        }
    }
}
