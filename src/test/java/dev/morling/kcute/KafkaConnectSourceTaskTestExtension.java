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
package dev.morling.kcute;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;

public class KafkaConnectSourceTaskTestExtension implements TaskRunner, Extension, BeforeEachCallback, AfterEachCallback {

    private final Class<? extends SourceTask> taskClass;
    private final Map<String, String> config;

    private final ExecutorService taskExecutor = Executors.newSingleThreadExecutor();
    private final BlockingQueue<SourceRecord> records = new ArrayBlockingQueue<>(2048);

    private SourceTask task;
    private AtomicBoolean running = new AtomicBoolean(false);
    private final Map<Map<String, ?>, Map<String, ?>> offsets;

    public KafkaConnectSourceTaskTestExtension(Class<? extends SourceTask> clazz, Map<String, String> config) {
        this.taskClass = clazz;
        this.config = config;
        this.offsets = new HashMap<>();
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        task = taskClass.getDeclaredConstructor().newInstance();
        start();
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        stop();
    }

    private void runTask() {
        while (running.get() == true) {
            try {
                List<SourceRecord> records = task.poll();

                for (SourceRecord sourceRecord : records) {
                    offsets.put(sourceRecord.sourcePartition(), sourceRecord.sourceOffset());
                }

                this.records.addAll(records);
            }
            catch (InterruptedException e) {
                Thread.interrupted();
                break;
            }
        }

        task.stop();
    }

    @Override
    public List<SourceRecord> take(String topic, int count) {
        List<SourceRecord> records = new ArrayList<>();
        int i = 0;

        while (i < count) {

            try {
                SourceRecord record = this.records.take();

                if (record.topic().equals(topic)) {
                    records.add(record);
                    i++;
                }
            }
            catch (InterruptedException e) {
                Thread.interrupted();
                throw new RuntimeException(e);
            }
        }

        return records;
    }

    @Override
    public void start() {
        task.initialize(new MockSourceTaskContext());
        task.start(config);
        running.set(true);
        taskExecutor.submit(() -> runTask());
    }

    @Override
    public void stop() {
        running.set(false);
    }

    private class MockSourceTaskContext implements SourceTaskContext {

        @Override
        public Map<String, String> configs() {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public OffsetStorageReader offsetStorageReader() {
            return new OffsetStorageReader() {

                @Override
                public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
                    throw new UnsupportedOperationException("Not implemented");
                }

                @Override
                public <T> Map<String, Object> offset(Map<String, T> partition) {
                    return (Map<String, Object>) offsets.get(partition);
                }
            };
        }
    }
}
