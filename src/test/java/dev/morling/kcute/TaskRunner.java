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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

public interface TaskRunner {

    public static TaskRunnerBuilder forSourceTask(Class<? extends SourceTask> clazz) {
        return new TaskRunnerBuilder(clazz);
    }

    public static class TaskRunnerBuilder {

        private final Class<? extends SourceTask> clazz;
        private final Map<String, String> config;

        public TaskRunnerBuilder(Class<? extends SourceTask> clazz) {
            this.clazz = clazz;
            this.config = new HashMap<>();
        }

        public TaskRunnerBuilder with(String key, String value) {
            config.put(key, value);
            return this;
        }

        public TaskRunner build() {
            return new KafkaConnectSourceTaskTestExtension(clazz, config);
        }
    }

    public List<SourceRecord> take(String topic, int count);

    public void stop();

    public void start();
}
