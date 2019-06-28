/*
 * Copyright 2019 ConsenSys AG.
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
package tech.pegasys.pantheon.cli.options;

import static org.mockito.Mockito.verify;

import tech.pegasys.pantheon.services.kvstore.RocksDbConfiguration;

import java.util.Arrays;
import java.util.List;

import org.mockito.ArgumentCaptor;

public class RocksDBOptionsTest
    extends AbstractCLIOptionsTest<RocksDbConfiguration, RocksDBOptions> {

  @Override
  RocksDbConfiguration createDefaultDomainObject() {
    return RocksDbConfiguration.builder().build();
  }

  @Override
  RocksDbConfiguration createCustomizedDomainObject() {
    return RocksDbConfiguration.builder()
        .maxOpenFiles(RocksDbConfiguration.DEFAULT_MAX_OPEN_FILES + 1)
        .cacheCapacity(RocksDbConfiguration.DEFAULT_CACHE_CAPACITY + 1)
        .maxBackgroundCompactions(RocksDbConfiguration.DEFAULT_MAX_BACKGROUND_COMPACTIONS + 1)
        .backgroundThreadCount(RocksDbConfiguration.DEFAULT_BACKGROUND_THREAD_COUNT + 1)
        .build();
  }

  @Override
  RocksDBOptions optionsFromDomainObject(final RocksDbConfiguration domainObject) {
    return RocksDBOptions.fromConfig(domainObject);
  }

  @Override
  RocksDbConfiguration optionsToDomainObject(final RocksDBOptions options) {
    return options.toDomainObject().build();
  }

  @Override
  RocksDbConfiguration getDomainObjectFromPantheonCommand() {
    final ArgumentCaptor<RocksDbConfiguration> captor =
        ArgumentCaptor.forClass(RocksDbConfiguration.class);
    verify(mockControllerBuilder).rocksDbConfiguration(captor.capture());
    return captor.getValue();
  }

  @Override
  protected List<String> getFieldsToIgnore() {
    return Arrays.asList("databaseDir");
  }
}
