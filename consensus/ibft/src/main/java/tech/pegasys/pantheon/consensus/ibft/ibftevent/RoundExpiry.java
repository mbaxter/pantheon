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
package tech.pegasys.pantheon.consensus.ibft.ibftevent;

import tech.pegasys.pantheon.consensus.ibft.ConsensusRoundIdentifier;
import tech.pegasys.pantheon.consensus.ibft.IbftEvent;
import tech.pegasys.pantheon.consensus.ibft.IbftEvents.Type;

import java.util.Objects;

import com.google.common.base.MoreObjects;

/** Event indicating a round timer has expired */
public final class RoundExpiry implements IbftEvent {
  private final ConsensusRoundIdentifier round;

  /**
   * Constructor for a RoundExpiry event
   *
   * @param round The round that the expired timer belonged to
   */
  public RoundExpiry(final ConsensusRoundIdentifier round) {
    this.round = round;
  }

  @Override
  public Type getType() {
    return Type.ROUND_EXPIRY;
  }

  public ConsensusRoundIdentifier getView() {
    return round;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("Round", round).toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final RoundExpiry that = (RoundExpiry) o;
    return Objects.equals(round, that.round);
  }

  @Override
  public int hashCode() {
    return Objects.hash(round);
  }
}
