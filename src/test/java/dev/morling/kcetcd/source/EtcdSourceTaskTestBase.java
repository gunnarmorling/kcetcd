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

import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.extension.RegisterExtension;

import dev.morling.kcute.TaskRunner;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.test.EtcdClusterExtension;

// TODO: jetcd 0.7.2 doesn't properly shut down the etcd container between multiple tests of one class;
// so we can only have one test method per class for now
public class EtcdSourceTaskTestBase {

    @RegisterExtension
    public static final EtcdClusterExtension etcd = EtcdClusterExtension.builder()
            .withNodes(1)
            .build();

    @RegisterExtension
    public TaskRunner taskRunner = TaskRunner.forSourceTask(EtcdSourceConnectorTask.class)
            .with("clusters", "test-etcd=" + etcd.clientEndpoints().get(0))
            .build();

    protected long getCurrentRevision(KV kvClient) throws InterruptedException, ExecutionException {
        return kvClient.get(ByteSequence.from("not-existent".getBytes()))
                .get()
                .getHeader()
                .getRevision();
    }
}
