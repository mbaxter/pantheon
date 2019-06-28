/*
 * Copyright 2018 ConsenSys AG.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package tech.pegasys.pantheon.services.kvstore;

import tech.pegasys.pantheon.metrics.noop.NoOpMetricsSystem;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class RocksDbKeyValueStorageTest extends AbstractKeyValueStorageTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Override
  protected KeyValueStorage createStore() throws Exception {
    return RocksDbKeyValueStorage.create(
        RocksDbConfiguration.builder().databaseDir(folder.newFolder().toPath()).build(),
        new NoOpMetricsSystem());
  }
}
