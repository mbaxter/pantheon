package tech.pegasys.pantheon.ethereum.eth.sync.worldstate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
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
  private Optional<CompletableFuture<Void>> future = Optional.empty();

  public WorldStateDownloader(EthContext ethContext, WorldStateStorage worldStateStorage, BlockHeader header, Queue pendingNodeQueue, final LabelledMetric<OperationTimer> ethTasksTimer) {
    this.ethContext = ethContext;
    this.header = header;
    this.pendingNodeQueue = pendingNodeQueue;
    this.ethTasksTimer = ethTasksTimer;
    // TODO: construct an updater that will commit changes periodically as updates accumulate
    this.worldStateStorageUpdater = worldStateStorage.updater();

    pendingNodeQueue.enqueue(NodeData.createAccountTrieNode(header.getStateRoot()));
  }

  public CompletableFuture<Void> run() {
    if (future.isPresent() && !future.get().isDone()) {
      return future.get();
    }
    future = Optional.of(new CompletableFuture<>());
    requestNodeData();

    // TODO: Complete exceptionally on timeout / stalled download
    return future.get();
  }

  private void requestNodeData() {
    while (outstandingRequests.get() < maxOutstandingRequests && !pendingNodeQueue.isEmpty()) {
      // Find available peer
      Optional<EthPeer> maybePeer = ethContext.getEthPeers().idlePeer(header.getNumber());

      if (!maybePeer.isPresent()) {
        // If no peer is available, wait and try again
        waitForNewPeer()
          .whenComplete((r,t) -> {
            requestNodeData();
          });
        return;
      }
      EthPeer peer = maybePeer.get();

      // Collect data to be requested
      List<NodeData> toRequest = new ArrayList<>();
      for(int i = 0; i < hashCountPerRequest; i++) {
        if (pendingNodeQueue.isEmpty()) {
          break;
        }
        toRequest.add(pendingNodeQueue.dequeue());
      }

      // Request and process node data
      outstandingRequests.incrementAndGet();
      sendAndProcessNodeDataRequest(peer, toRequest)
        .thenAccept((res) -> {
          if (outstandingRequests.decrementAndGet() == 0 && pendingNodeQueue.isEmpty()) {
            // We're done
            future.get().complete(null);
          }
        });
    }
  }

  private CompletableFuture<?> waitForNewPeer() {
    return ethContext
      .getScheduler()
      .timeout(WaitForPeerTask.create(ethContext, ethTasksTimer), Duration.ofSeconds(5));
  }

  private CompletableFuture<?> sendAndProcessNodeDataRequest(EthPeer peer, List<NodeData> nodeData) {
    List<Hash> hashes = nodeData.stream().map(NodeData::getHash).collect(Collectors.toList());
    return GetNodeDataFromPeerTask.forHashes(ethContext, hashes, ethTasksTimer).assignPeer(peer).run()
      .thenApply(PeerTaskResult::getResult)
      // TODO: Update task to return this mapping
      .thenApply(this::mapNodeDataByHash)
      .thenAccept(data -> {
        for (NodeData nodeDatum : nodeData) {
          BytesValue matchingData = data.get(nodeDatum.getHash());
          if (matchingData == null) {
            pendingNodeQueue.enqueue(nodeDatum);
          } else {
            // Persist node
            nodeDatum.setData(matchingData);
            nodeDatum.persist(worldStateStorageUpdater);
            // TODO: Process node value and/or children
          }
        }
      });
  }

  private Map<Hash, BytesValue> mapNodeDataByHash(List<BytesValue> data) {
    // Map data by hash
    Map<Hash, BytesValue> dataByHash = new HashMap<>();
    data.stream()
      .forEach(d -> dataByHash.put(Hash.hash(d), d));
    return dataByHash;
  }
}
