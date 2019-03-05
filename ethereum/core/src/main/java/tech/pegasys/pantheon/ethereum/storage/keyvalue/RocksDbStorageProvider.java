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
package tech.pegasys.pantheon.ethereum.storage.keyvalue;

import tech.pegasys.pantheon.ethereum.storage.StorageProvider;
import tech.pegasys.pantheon.metrics.MetricsSystem;
import tech.pegasys.pantheon.services.kvstore.KeyValueStorage;
import tech.pegasys.pantheon.services.kvstore.RocksDbKeyValueStorage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;

public class RocksDbStorageProvider {
  private static String MONOLITHIC_DATABASE_PATH = "database";
  private static String WORLDSTATE_DATABASE_PATH = "chain";
  private static String CHAIN_DATABASE_PATH = "worldstate";

  public static StorageProvider create(final Path dataDirectory, final MetricsSystem metricsSystem)
      throws IOException {
    // Make sure data directory exists
    Files.createDirectories(dataDirectory);

    if (dataDirectory.resolve(MONOLITHIC_DATABASE_PATH).toFile().exists()) {
      // If database has already been initialized as a monolithic db, keep this structure
      final Supplier<KeyValueStorage> stroageSupplier =
          getMonolithicStorageSupplier(dataDirectory, metricsSystem);
      return new KeyValueStorageProvider(stroageSupplier, stroageSupplier);
    } else {
      // Otherwise setup separate world state and chain databases
      final Supplier<KeyValueStorage> worldStateStorage =
          getWorldStateStorageSupplier(dataDirectory, metricsSystem);
      final Supplier<KeyValueStorage> chainStorage =
          getChainStorageSupplier(dataDirectory, metricsSystem);
      return new KeyValueStorageProvider(worldStateStorage, chainStorage);
    }
  }

  private static Supplier<KeyValueStorage> getMonolithicStorageSupplier(
      final Path dataDirectory, final MetricsSystem metricsSystem) {
    return Suppliers.memoize(
        () ->
            RocksDbKeyValueStorage.create(
                dataDirectory.resolve(MONOLITHIC_DATABASE_PATH), metricsSystem));
  }

  private static Supplier<KeyValueStorage> getWorldStateStorageSupplier(
      final Path dataDirectory, final MetricsSystem metricsSystem) {
    return Suppliers.memoize(
        () ->
            RocksDbKeyValueStorage.create(
                dataDirectory.resolve(WORLDSTATE_DATABASE_PATH), metricsSystem));
  }

  private static Supplier<KeyValueStorage> getChainStorageSupplier(
      final Path dataDirectory, final MetricsSystem metricsSystem) {
    return Suppliers.memoize(
        () ->
            RocksDbKeyValueStorage.create(
                dataDirectory.resolve(CHAIN_DATABASE_PATH), metricsSystem));
  }
}
