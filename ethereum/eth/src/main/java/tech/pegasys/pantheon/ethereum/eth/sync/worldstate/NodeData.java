package tech.pegasys.pantheon.ethereum.eth.sync.worldstate;

import static com.google.common.base.Preconditions.checkNotNull;

import tech.pegasys.pantheon.ethereum.core.Hash;
import tech.pegasys.pantheon.ethereum.worldstate.WorldStateStorage;
import tech.pegasys.pantheon.ethereum.worldstate.WorldStateStorage.Updater;
import tech.pegasys.pantheon.util.bytes.BytesValue;

public abstract class NodeData {
  public enum Kind {
    ACCOUNT_TRIE_NODE,
    STORAGE_TRIE_NODE,
    CODE
  }

  private final Kind kind;
  private final Hash hash;
  private BytesValue data;

  protected NodeData(Kind kind, Hash hash) {
    this.kind = kind;
    this.hash = hash;
  }

  public static AccountTrieNodeData createAccountTrieNode(Hash hash) {
    return new AccountTrieNodeData(hash);
  }

  public static StorageTrieNodeData createStorageTrieNode(Hash hash) {
    return new StorageTrieNodeData(hash);
  }

  public static CodeNodeData createCodeNode(Hash hash) {
    return new CodeNodeData(hash);
  }

  public Kind getKind() {
    return kind;
  }

  public Hash getHash() {
    return hash;
  }

  public BytesValue getData() {
    return data;
  }

  public NodeData setData(BytesValue data) {
    this.data = data;
    return this;
  }

  abstract void persist(WorldStateStorage.Updater updater);


  public static class AccountTrieNodeData extends NodeData {

    public AccountTrieNodeData(Hash hash) {
      super(Kind.ACCOUNT_TRIE_NODE, hash);
    }

    @Override
    void persist(Updater updater) {
      checkNotNull(getData(), "Must set data before node can be persisted.");
      updater.putAccountStateTrieNode(getHash(), getData());
    }
  }

  public static class StorageTrieNodeData extends NodeData {

    public StorageTrieNodeData(Hash hash) {
      super(Kind.STORAGE_TRIE_NODE, hash);
    }

    @Override
    void persist(Updater updater) {
      checkNotNull(getData(), "Must set data before node can be persisted.");
      updater.putAccountStorageTrieNode(getHash(), getData());
    }
  }

  public static class CodeNodeData extends NodeData {

    public CodeNodeData(Hash hash) {
      super(Kind.CODE, hash);
    }

    @Override
    void persist(Updater updater) {
      checkNotNull(getData(), "Must set data before node can be persisted.");
      updater.putCode(getHash(), getData());
    }
  }
}
