package tech.pegasys.pantheon.ethereum.eth.sync.worldstate;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import tech.pegasys.pantheon.ethereum.core.BlockDataGenerator;

public class NodeDataRequestTest {

  @Test
  public void serializesAccountTrieNodeRequests() {
    BlockDataGenerator gen = new BlockDataGenerator(0);
    AccountTrieNodeDataRequest request = NodeDataRequest.createAccountDataRequest(gen.hash());
    NodeDataRequest sedeRequest = serializeThenDeserialize(request);
    assertRequestsEquals(sedeRequest, request);
    assertThat(sedeRequest).isInstanceOf(AccountTrieNodeDataRequest.class);
  }

  @Test
  public void serializesStorageTrieNodeRequests() {
    BlockDataGenerator gen = new BlockDataGenerator(0);
    StorageTrieNodeDataRequest request = NodeDataRequest.createStorageDataRequest(gen.hash());
    NodeDataRequest sedeRequest = serializeThenDeserialize(request);
    assertRequestsEquals(sedeRequest, request);
    assertThat(sedeRequest).isInstanceOf(StorageTrieNodeDataRequest.class);
  }

  @Test
  public void serializesCodeRequests() {
    BlockDataGenerator gen = new BlockDataGenerator(0);
    CodeNodeDataRequest request = NodeDataRequest.createCodeRequest(gen.hash());
    NodeDataRequest sedeRequest = serializeThenDeserialize(request);
    assertRequestsEquals(sedeRequest, request);
    assertThat(sedeRequest).isInstanceOf(CodeNodeDataRequest.class);
  }

  private NodeDataRequest serializeThenDeserialize(NodeDataRequest request) {
    return NodeDataRequest.deserialize(NodeDataRequest.serialize(request));
  }

  private void assertRequestsEquals(NodeDataRequest actual, NodeDataRequest expected) {
    assertThat(actual.getRequestType()).isEqualTo(expected.getRequestType());
    assertThat(actual.getHash()).isEqualTo(expected.getHash());
    assertThat(actual.getData()).isEqualTo(expected.getData());
  }
}
