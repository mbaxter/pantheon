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
package tech.pegasys.pantheon.consensus.ibft.validation;

import tech.pegasys.pantheon.consensus.ibft.messagewrappers.Proposal;
import tech.pegasys.pantheon.ethereum.core.BlockHeader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/* One of these will be created by the IbftBlockHeightManager and will exist for the life of the
chainheight, and used to ensure supplied Proposals are suitable for starting a new round.
 */
public class FutureRoundProposalMessageValidator {

  private static final Logger LOG = LogManager.getLogger();

  private final MessageValidatorFactory messageValidatorFactory;
  private final long chainHeight;
  private final BlockHeader parentHeader;

  public FutureRoundProposalMessageValidator(
      final MessageValidatorFactory messageValidatorFactory,
      final long chainHeight,
      final BlockHeader parentHeader) {
    this.messageValidatorFactory = messageValidatorFactory;
    this.chainHeight = chainHeight;
    this.parentHeader = parentHeader;
  }

  public boolean validateProposalMessage(final Proposal msg) {

    if (msg.getRoundIdentifier().getSequenceNumber() != chainHeight) {
      LOG.debug("Illegal Proposal message, does not target the correct round height.");
      return false;
    }

    final MessageValidator messageValidator =
        messageValidatorFactory.createMessageValidator(msg.getRoundIdentifier(), parentHeader);

    return messageValidator.validateProposal(msg);
  }
}
