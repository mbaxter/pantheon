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
package tech.pegasys.pantheon.tests.acceptance.dsl.jsonrpc;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

import tech.pegasys.pantheon.ethereum.permissioning.WhitelistPersistor.WHITELIST_TYPE;
import tech.pegasys.pantheon.tests.acceptance.dsl.condition.Condition;
import tech.pegasys.pantheon.tests.acceptance.dsl.condition.perm.AddAccountsToWhitelistSuccessfully;
import tech.pegasys.pantheon.tests.acceptance.dsl.condition.perm.AddNodeSuccess;
import tech.pegasys.pantheon.tests.acceptance.dsl.condition.perm.GetExpectedAccountsWhitelist;
import tech.pegasys.pantheon.tests.acceptance.dsl.condition.perm.GetNodesWhitelistPopulated;
import tech.pegasys.pantheon.tests.acceptance.dsl.condition.perm.RemoveAccountsFromWhitelistSuccessfully;
import tech.pegasys.pantheon.tests.acceptance.dsl.condition.perm.RemoveNodeSuccess;
import tech.pegasys.pantheon.tests.acceptance.dsl.condition.perm.WhiteListContainsKeyAndValue;
import tech.pegasys.pantheon.tests.acceptance.dsl.node.Node;
import tech.pegasys.pantheon.tests.acceptance.dsl.node.RunnableNode;
import tech.pegasys.pantheon.tests.acceptance.dsl.transaction.Transactions;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public class Perm {

  public Perm(final Transactions transactions) {
    this.transactions = transactions;
  }

  private final Transactions transactions;

  public Condition addAccountsToWhitelist(final String... accounts) {
    return new AddAccountsToWhitelistSuccessfully(transactions.addAccountsToWhitelist(accounts));
  }

  public Condition removeAccountsFromWhitelist(final String... accounts) {
    return new RemoveAccountsFromWhitelistSuccessfully(
        transactions.removeAccountsFromWhitelist(accounts));
  }

  public Condition expectAccountsWhitelist(final String... expectedAccounts) {
    return new GetExpectedAccountsWhitelist(
        transactions.getAccountsWhiteList(), asList(expectedAccounts));
  }

  public Condition addNodesToWhitelist(final String... nodes) {
    return addNodesToWhitelist(Stream.of(nodes).map(URI::create).collect(toList()));
  }

  public Condition addNodesToWhitelist(final Node... nodes) {
    final List<URI> enodeList = toEnodeUris(nodes);
    return addNodesToWhitelist(enodeList);
  }

  private Condition addNodesToWhitelist(final List<URI> enodeList) {
    return new AddNodeSuccess(transactions.addNodesToWhitelist(enodeList));
  }

  public Condition removeNodesFromWhitelist(final String... nodes) {
    return removeNodesFromWhitelist(Stream.of(nodes).map(URI::create).collect(toList()));
  }

  public Condition removeNodesFromWhitelist(final Node... nodes) {
    final List<URI> enodeList = toEnodeUris(nodes);
    return removeNodesFromWhitelist(enodeList);
  }

  private Condition removeNodesFromWhitelist(final List<URI> enodeList) {
    return new RemoveNodeSuccess(transactions.removeNodesFromWhitelist(enodeList));
  }

  public Condition getNodesWhitelist(final int expectedNodeNum) {
    return new GetNodesWhitelistPopulated(transactions.getNodesWhiteList(), expectedNodeNum);
  }

  public Condition expectPermissioningWhitelistFileKeyValue(
      final WHITELIST_TYPE whitelistType, final Path configFilePath, final String... val) {
    return new WhiteListContainsKeyAndValue(whitelistType, asList(val), configFilePath);
  }

  private List<URI> toEnodeUris(final Node[] nodes) {
    return Stream.of(nodes)
        .map(node -> (RunnableNode) node)
        .map(RunnableNode::enodeUrl)
        .collect(toList());
  }
}
