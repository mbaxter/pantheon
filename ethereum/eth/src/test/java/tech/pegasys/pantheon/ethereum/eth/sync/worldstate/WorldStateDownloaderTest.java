package tech.pegasys.pantheon.ethereum.eth.sync.worldstate;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.BeforeClass;
import org.junit.Test;
import tech.pegasys.pantheon.ethereum.ProtocolContext;
import tech.pegasys.pantheon.ethereum.chain.MutableBlockchain;
import tech.pegasys.pantheon.ethereum.core.BlockHeader;
import tech.pegasys.pantheon.ethereum.eth.manager.EthProtocolManager;
import tech.pegasys.pantheon.ethereum.eth.manager.EthProtocolManagerTestUtil;
import tech.pegasys.pantheon.ethereum.eth.manager.RespondingEthPeer;
import tech.pegasys.pantheon.ethereum.eth.manager.RespondingEthPeer.Responder;
import tech.pegasys.pantheon.ethereum.eth.manager.ethtaskutils.BlockchainSetupUtil;
import tech.pegasys.pantheon.ethereum.mainnet.ProtocolSchedule;
import tech.pegasys.pantheon.ethereum.storage.keyvalue.KeyValueStorageWorldStateStorage;
import tech.pegasys.pantheon.ethereum.worldstate.WorldStateArchive;
import tech.pegasys.pantheon.ethereum.worldstate.WorldStateStorage;
import tech.pegasys.pantheon.metrics.LabelledMetric;
import tech.pegasys.pantheon.metrics.OperationTimer;
import tech.pegasys.pantheon.metrics.noop.NoOpMetricsSystem;
import tech.pegasys.pantheon.services.kvstore.InMemoryKeyValueStorage;
import tech.pegasys.pantheon.services.queue.InMemoryQueue;
import tech.pegasys.pantheon.services.queue.Queue;

public class WorldStateDownloaderTest {

  private static WorldStateArchive remoteWorldArchive;
  private static MutableBlockchain remoteBlockchain;
  private static ProtocolSchedule<Void> protocolSchedule;
  private static ProtocolContext<Void> protocolContext;
  private static LabelledMetric<OperationTimer> ethTasksTimer;
  private static EthProtocolManager ethProtocolManager;

  @BeforeClass
  public static void setup() {
    final BlockchainSetupUtil<Void> blockchainSetupUtil = BlockchainSetupUtil.forTesting();
    blockchainSetupUtil.importAllBlocks();
    remoteBlockchain = blockchainSetupUtil.getBlockchain();
    remoteWorldArchive = blockchainSetupUtil.getWorldArchive();
    protocolSchedule = blockchainSetupUtil.getProtocolSchedule();
    protocolContext = blockchainSetupUtil.getProtocolContext();
    ethTasksTimer = NoOpMetricsSystem.NO_OP_LABELLED_TIMER;
    ethProtocolManager = EthProtocolManagerTestUtil.create();
  }

  @Test
  public void downloadAvailableWorldStateFromPeers() {
    // Pull chain head and a prior header with a different state root
    BlockHeader chainHead = remoteBlockchain.getChainHeadHeader();
    BlockHeader header = remoteBlockchain.getBlockHeader(chainHead.getNumber() - 1).get();
    assertThat(chainHead.getStateRoot()).isNotEqualTo(header.getStateRoot());

    Queue<NodeData> queue = new InMemoryQueue<>();
    WorldStateStorage stateStorage = new KeyValueStorageWorldStateStorage(new InMemoryKeyValueStorage());
    WorldStateDownloader downloader = new WorldStateDownloader(ethProtocolManager.ethContext(), stateStorage, header, queue, ethTasksTimer);

    // Create some peers that can respond
    int peerCount = 5;
    List<RespondingEthPeer> peers = Stream.generate(() -> EthProtocolManagerTestUtil.createPeer(ethProtocolManager, remoteBlockchain))
      .limit(peerCount)
      .collect(Collectors.toList());

    // Start downloader
    CompletableFuture<?> result = downloader.run();

    // Respond to node data requests
    Responder responder = RespondingEthPeer.blockchainResponder(remoteBlockchain, remoteWorldArchive);
    while (!result.isDone()) {
      for (RespondingEthPeer peer : peers) {
        peer.respond(responder);
      }
    }

    assertThat(result).isDone();
    assertThat(stateStorage.contains(chainHead.getStateRoot())).isFalse();
  }


}
