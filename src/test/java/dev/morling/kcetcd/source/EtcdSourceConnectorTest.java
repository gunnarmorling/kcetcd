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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

public class EtcdSourceConnectorTest {

    @Test
    public void shouldCreateConfigurationForTasks() throws Exception {
        EtcdSourceConnector connector = new EtcdSourceConnector();
        Map<String, String> config = new HashMap<>();
        config.put("clusters", "etcd-a=http://etcd-a-1:2379,http://etcd-a-2:2379,http://etcd-a-3:2379;etcd-b=http://etcd-b-1:2379;etcd-c=http://etcd-c-1:2379");
        connector.start(config);

        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertThat(taskConfigs).hasSize(1);

        Map<String, String> taskConfig = taskConfigs.get(0);
        assertThat(taskConfig).containsEntry("clusters",
                "etcd-a=http://etcd-a-1:2379,http://etcd-a-2:2379,http://etcd-a-3:2379;etcd-b=http://etcd-b-1:2379;etcd-c=http://etcd-c-1:2379");

        taskConfigs = connector.taskConfigs(2);
        assertThat(taskConfigs).hasSize(2);

        taskConfig = taskConfigs.get(0);
        assertThat(taskConfig).containsEntry("clusters", "etcd-a=http://etcd-a-1:2379,http://etcd-a-2:2379,http://etcd-a-3:2379;etcd-b=http://etcd-b-1:2379");

        taskConfig = taskConfigs.get(1);
        assertThat(taskConfig).containsEntry("clusters", "etcd-c=http://etcd-c-1:2379");

        taskConfigs = connector.taskConfigs(3);
        assertThat(taskConfigs).hasSize(3);

        taskConfig = taskConfigs.get(0);
        assertThat(taskConfig).containsEntry("clusters", "etcd-a=http://etcd-a-1:2379,http://etcd-a-2:2379,http://etcd-a-3:2379");

        taskConfig = taskConfigs.get(1);
        assertThat(taskConfig).containsEntry("clusters", "etcd-b=http://etcd-b-1:2379");

        taskConfig = taskConfigs.get(2);
        assertThat(taskConfig).containsEntry("clusters", "etcd-c=http://etcd-c-1:2379");
    }
}
