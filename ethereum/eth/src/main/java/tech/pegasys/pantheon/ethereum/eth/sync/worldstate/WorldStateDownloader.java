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
package tech.pegasys.pantheon.ethereum.eth.sync.worldstate;

import tech.pegasys.pantheon.ethereum.core.BlockHeader;
import tech.pegasys.pantheon.ethereum.core.Hash;
import tech.pegasys.pantheon.ethereum.eth.manager.AbstractPeerTask.PeerTaskResult;
import tech.pegasys.pantheon.ethereum.eth.manager.EthContext;
import tech.pegasys.pantheon.ethereum.eth.manager.EthPeer;
import tech.pegasys.pantheon.ethereum.eth.sync.tasks.GetNodeDataFromPeerTask;
import tech.pegasys.pantheon.ethereum.eth.sync.tasks.WaitForPeerTask;
import tech.pegasys.pantheon.ethereum.worldstate.WorldStateStorage;
import tech.pegasys.pantheon.metrics.LabelledMetric;
import tech.pegasys.pantheon.metrics.OperationTimer;
import tech.pegasys.pantheon.services.queue.Queue;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class WorldStateDownloader {

  private final EthContext ethContext;
  // The target header for which we want to retrieve world state
  private final BlockHeader header;
  private final Queue<NodeData> pendingNodeQueue;
  private final WorldStateStorage.Updater worldStateStorageUpdater;
  private final int hashCountPerRequest = 50;
  private final int maxOutstandingRequests = 50;
  private final AtomicInteger outstandingRequests = new AtomicInteger(0);
  private final LabelledMetric<OperationTimer> ethTasksTimer;
  private final WorldStateStorage worldStateStorage;
  private final AtomicReference<CompletableFuture<Void>> future =
      new AtomicReference<CompletableFuture<Void>>(null);

  public WorldStateDownloader(
      EthContext ethContext,
      WorldStateStorage worldStateStorage,
      BlockHeader header,
      Queue pendingNodeQueue,
      final LabelledMetric<OperationTimer> ethTasksTimer) {
    this.ethContext = ethContext;
    this.worldStateStorage = worldStateStorage;
    this.header = header;
    this.pendingNodeQueue = pendingNodeQueue;
    this.ethTasksTimer = ethTasksTimer;
    // TODO: construct an updater that will commit changes periodically as updates accumulate
    this.worldStateStorageUpdater = worldStateStorage.updater();

    pendingNodeQueue.enqueue(NodeData.createAccountTrieNode(header.getStateRoot()));
  }

  public CompletableFuture<Void> run() {
    if (future.get() != null && !future.get().isDone()) {
      return future.get();
    }
    future.set(new CompletableFuture<>());
    requestNodeData();

    // TODO: Complete exceptionally on timeout / stalled download
    return future.get();
  }

  private void requestNodeData() {
    while (!future.get().isDone()
        && outstandingRequests.get() < maxOutstandingRequests
        && !pendingNodeQueue.isEmpty()) {
      // Find available peer
      Optional<EthPeer> maybePeer = ethContext.getEthPeers().idlePeer(header.getNumber());

      if (!maybePeer.isPresent()) {
        // If no peer is available, wait and try again
        waitForNewPeer()
            .whenComplete(
                (r, t) -> {
                  requestNodeData();
                });
        return;
      }
      EthPeer peer = maybePeer.get();

      // Collect data to be requested
      List<NodeData> toRequest = new ArrayList<>();
      for (int i = 0; i < hashCountPerRequest; i++) {
        if (pendingNodeQueue.isEmpty()) {
          break;
        }
        toRequest.add(pendingNodeQueue.dequeue());
      }

      // Request and process node data
      outstandingRequests.incrementAndGet();
      sendAndProcessNodeDataRequest(peer, toRequest)
          .whenComplete(
              (res, error) -> {
                if (outstandingRequests.decrementAndGet() == 0 && pendingNodeQueue.isEmpty()) {
                  // We're done
                  future.get().complete(null);
                } else {
                  // Send out additional requests
                  requestNodeData();
                }
              });
    }
  }

  private CompletableFuture<?> waitForNewPeer() {
    return ethContext
        .getScheduler()
        .timeout(WaitForPeerTask.create(ethContext, ethTasksTimer), Duration.ofSeconds(5));
  }

  private CompletableFuture<?> sendAndProcessNodeDataRequest(
      EthPeer peer, List<NodeData> nodeData) {
    List<Hash> hashes = nodeData.stream().map(NodeData::getHash).collect(Collectors.toList());
    return GetNodeDataFromPeerTask.forHashes(ethContext, hashes, ethTasksTimer)
        .assignPeer(peer)
        .run()
        .thenApply(PeerTaskResult::getResult)
        // TODO: Update task to return this mapping
        .thenApply(this::mapNodeDataByHash)
        .thenAccept(
            data -> {
              for (NodeData nodeDatum : nodeData) {
                BytesValue matchingData = data.get(nodeDatum.getHash());
                if (matchingData == null) {
                  pendingNodeQueue.enqueue(nodeDatum);
                } else {
                  // Persist node
                  nodeDatum.setData(matchingData);
                  nodeDatum.persist(worldStateStorageUpdater);

                  // Queue child requests
                  nodeDatum
                      .getChildNodeData()
                      .filter(n -> !worldStateStorage.contains(n.getHash()))
                      .forEach(pendingNodeQueue::enqueue);
                }
              }
            });
  }

  private Map<Hash, BytesValue> mapNodeDataByHash(List<BytesValue> data) {
    // Map data by hash
    Map<Hash, BytesValue> dataByHash = new HashMap<>();
    data.stream().forEach(d -> dataByHash.put(Hash.hash(d), d));
    return dataByHash;
  }
}
