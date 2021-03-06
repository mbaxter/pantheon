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
package tech.pegasys.pantheon.consensus.ibft;

import tech.pegasys.pantheon.consensus.common.BlockInterface;
import tech.pegasys.pantheon.consensus.common.ValidatorVote;
import tech.pegasys.pantheon.consensus.common.VoteType;
import tech.pegasys.pantheon.ethereum.core.Address;
import tech.pegasys.pantheon.ethereum.core.Block;
import tech.pegasys.pantheon.ethereum.core.BlockHeader;
import tech.pegasys.pantheon.ethereum.core.BlockHeaderBuilder;
import tech.pegasys.pantheon.ethereum.core.BlockHeaderFunctions;

import java.util.Collection;
import java.util.Optional;

public class IbftBlockInterface implements BlockInterface {

  @Override
  public Address getProposerOfBlock(final BlockHeader header) {
    return header.getCoinbase();
  }

  @Override
  public Optional<ValidatorVote> extractVoteFromHeader(final BlockHeader header) {
    final IbftExtraData ibftExtraData = IbftExtraData.decode(header);

    if (ibftExtraData.getVote().isPresent()) {
      final Vote headerVote = ibftExtraData.getVote().get();
      final ValidatorVote vote =
          new ValidatorVote(
              headerVote.isAuth() ? VoteType.ADD : VoteType.DROP,
              getProposerOfBlock(header),
              headerVote.getRecipient());
      return Optional.of(vote);
    }
    return Optional.empty();
  }

  @Override
  public Collection<Address> validatorsInBlock(final BlockHeader header) {
    final IbftExtraData ibftExtraData = IbftExtraData.decode(header);
    return ibftExtraData.getValidators();
  }

  public static Block replaceRoundInBlock(
      final Block block, final int round, final BlockHeaderFunctions blockHeaderFunctions) {
    final IbftExtraData prevExtraData = IbftExtraData.decode(block.getHeader());
    final IbftExtraData substituteExtraData =
        new IbftExtraData(
            prevExtraData.getVanityData(),
            prevExtraData.getSeals(),
            prevExtraData.getVote(),
            round,
            prevExtraData.getValidators());

    final BlockHeaderBuilder headerBuilder = BlockHeaderBuilder.fromHeader(block.getHeader());
    headerBuilder
        .extraData(substituteExtraData.encode())
        .blockHeaderFunctions(blockHeaderFunctions);

    final BlockHeader newHeader = headerBuilder.buildBlockHeader();

    return new Block(newHeader, block.getBody());
  }
}
