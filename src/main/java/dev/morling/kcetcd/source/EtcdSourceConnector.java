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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

public class EtcdSourceConnector extends SourceConnector {

    private static final String ENDPOINTS = "endpoints";
    private String endpoints;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        System.out.println("Starting connector");
        this.endpoints = props.get(ENDPOINTS);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return EtcdSourceConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        String[] partitionsArray = endpoints.split(",");
        int chunkSize = ceilDiv(partitionsArray.length, maxTasks);

        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < partitionsArray.length; i += chunkSize) {
            String[] chunk = Arrays.copyOfRange(partitionsArray, i, Math.min(partitionsArray.length, i + chunkSize));

            Map<String, String> config = new HashMap<>();
            config.put(ENDPOINTS, String.join(",", chunk));
            configs.add(config);
        }

        return configs;
    }

    @Override
    public void stop() {
        System.out.println("Stopping connector");
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef().define(ENDPOINTS, Type.STRING, Importance.HIGH, "etcd cluster endpoint(s)");
    }

    private int ceilDiv(int x, int y) {
        return -Math.floorDiv(-x, y);
    }
}
