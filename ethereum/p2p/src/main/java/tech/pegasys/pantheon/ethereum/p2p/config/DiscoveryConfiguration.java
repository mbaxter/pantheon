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
package tech.pegasys.pantheon.ethereum.p2p.config;

import static java.util.stream.Collectors.toList;

import tech.pegasys.pantheon.ethereum.p2p.peers.EnodeURL;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DiscoveryConfiguration {
  public static List<EnodeURL> MAINNET_BOOTSTRAP_NODES =
      Collections.unmodifiableList(
          Stream.of(
                  "enode://a979fb575495b8d6db44f750317d0f4622bf4c2aa3365d6af7c284339968eef29b69ad0dce72a4d8db5ebb4968de0e3bec910127f134779fbcb0cb6d3331163c@52.16.188.185:30303",
                  "enode://3f1d12044546b76342d59d4a05532c14b85aa669704bfe1f864fe079415aa2c02d743e03218e57a33fb94523adb54032871a6c51b2cc5514cb7c7e35b3ed0a99@13.93.211.84:30303",
                  "enode://78de8a0916848093c73790ead81d1928bec737d565119932b98c6b100d944b7a95e94f847f689fc723399d2e31129d182f7ef3863f2b4c820abbf3ab2722344d@191.235.84.50:30303",
                  "enode://158f8aab45f6d19c6cbf4a089c2670541a8da11978a2f90dbf6a502a4a3bab80d288afdbeb7ec0ef6d92de563767f3b1ea9e8e334ca711e9f8e2df5a0385e8e6@13.75.154.138:30303",
                  "enode://1118980bf48b0a3640bdba04e0fe78b1add18e1cd99bf22d53daac1fd9972ad650df52176e7c7d89d1114cfef2bc23a2959aa54998a46afcf7d91809f0855082@52.74.57.123:30303",
                  "enode://979b7fa28feeb35a4741660a16076f1943202cb72b6af70d327f053e248bab9ba81760f39d0701ef1d8f89cc1fbd2cacba0710a12cd5314d5e0c9021aa3637f9@5.1.83.226:30303")
              .map(EnodeURL::fromString)
              .collect(toList()));
  public static List<EnodeURL> RINKEBY_BOOTSTRAP_NODES =
      Collections.unmodifiableList(
          Stream.of(
                  "enode://a24ac7c5484ef4ed0c5eb2d36620ba4e4aa13b8c84684e1b4aab0cebea2ae45cb4d375b77eab56516d34bfbd3c1a833fc51296ff084b770b94fb9028c4d25ccf@52.169.42.101:30303",
                  "enode://343149e4feefa15d882d9fe4ac7d88f885bd05ebb735e547f12e12080a9fa07c8014ca6fd7f373123488102fe5e34111f8509cf0b7de3f5b44339c9f25e87cb8@52.3.158.184:30303",
                  "enode://b6b28890b006743680c52e64e0d16db57f28124885595fa03a562be1d2bf0f3a1da297d56b13da25fb992888fd556d4c1a27b1f39d531bde7de1921c90061cc6@159.89.28.211:30303")
              .map(EnodeURL::fromString)
              .collect(toList()));
  public static List<EnodeURL> ROPSTEN_BOOTSTRAP_NODES =
      Collections.unmodifiableList(
          Stream.of(
                  "enode://6332792c4a00e3e4ee0926ed89e0d27ef985424d97b6a45bf0f23e51f0dcb5e66b875777506458aea7af6f9e4ffb69f43f3778ee73c81ed9d34c51c4b16b0b0f@52.232.243.152:30303",
                  "enode://94c15d1b9e2fe7ce56e458b9a3b672ef11894ddedd0c6f247e0f1d3487f52b66208fb4aeb8179fce6e3a749ea93ed147c37976d67af557508d199d9594c35f09@192.81.208.223:30303",
                  "enode://30b7ab30a01c124a6cceca36863ece12c4f5fa68e3ba9b0b51407ccc002eeed3b3102d20a88f1c1d3c3154e2449317b8ef95090e77b312d5cc39354f86d5d606@52.176.7.10:30303",
                  "enode://865a63255b3bb68023b6bffd5095118fcc13e79dcf014fe4e47e065c350c7cc72af2e53eff895f11ba1bbb6a2b33271c1116ee870f266618eadfc2e78aa7349c@52.176.100.77:30303")
              .map(EnodeURL::fromString)
              .collect(toList()));

  public static List<EnodeURL> GOERLI_BOOTSTRAP_NODES =
      Collections.unmodifiableList(
          Stream.of(
                  "enode://011f758e6552d105183b1761c5e2dea0111bc20fd5f6422bc7f91e0fabbec9a6595caf6239b37feb773dddd3f87240d99d859431891e4a642cf2a0a9e6cbb98a@51.141.78.53:30303",
                  "enode://176b9417f511d05b6b2cf3e34b756cf0a7096b3094572a8f6ef4cdcb9d1f9d00683bf0f83347eebdf3b81c3521c2332086d9592802230bf528eaf606a1d9677b@13.93.54.137:30303",
                  "enode://46add44b9f13965f7b9875ac6b85f016f341012d84f975377573800a863526f4da19ae2c620ec73d11591fa9510e992ecc03ad0751f53cc02f7c7ed6d55c7291@94.237.54.114:30313",
                  "enode://c1f8b7c2ac4453271fa07d8e9ecf9a2e8285aa0bd0c07df0131f47153306b0736fd3db8924e7a9bf0bed6b1d8d4f87362a71b033dc7c64547728d953e43e59b2@52.64.155.147:30303",
                  "enode://f4a9c6ee28586009fb5a96c8af13a58ed6d8315a9eee4772212c1d4d9cebe5a8b8a78ea4434f318726317d04a3f531a1ef0420cf9752605a562cfe858c46e263@213.186.16.82:30303")
              .map(EnodeURL::fromString)
              .collect(toList()));

  private boolean active = true;
  private String bindHost = "0.0.0.0";
  private int bindPort = 30303;
  private String advertisedHost = "127.0.0.1";
  private int bucketSize = 16;
  private List<EnodeURL> bootnodes = new ArrayList<>();

  public static DiscoveryConfiguration create() {
    return new DiscoveryConfiguration();
  }

  public static void assertValidBootnodes(final List<EnodeURL> bootnodes) {
    final List<EnodeURL> invalidEnodes =
        bootnodes.stream().filter(e -> !e.isRunningDiscovery()).collect(Collectors.toList());

    if (invalidEnodes.size() > 0) {
      String invalidBootnodes =
          invalidEnodes.stream().map(EnodeURL::toString).collect(Collectors.joining(","));
      String errorMsg =
          "Bootnodes must have discovery enabled. Invalid bootnodes: " + invalidBootnodes + ".";
      throw new IllegalArgumentException(errorMsg);
    }
  }

  public String getBindHost() {
    return bindHost;
  }

  public DiscoveryConfiguration setBindHost(final String bindHost) {
    this.bindHost = bindHost;
    return this;
  }

  public int getBindPort() {
    return bindPort;
  }

  public DiscoveryConfiguration setBindPort(final int bindPort) {
    this.bindPort = bindPort;
    return this;
  }

  public boolean isActive() {
    return active;
  }

  public DiscoveryConfiguration setActive(final boolean active) {
    this.active = active;
    return this;
  }

  public List<EnodeURL> getBootnodes() {
    return bootnodes;
  }

  public DiscoveryConfiguration setBootnodes(final List<EnodeURL> bootnodes) {
    assertValidBootnodes(bootnodes);
    this.bootnodes = bootnodes;
    return this;
  }

  public String getAdvertisedHost() {
    return advertisedHost;
  }

  public DiscoveryConfiguration setAdvertisedHost(final String advertisedHost) {
    this.advertisedHost = advertisedHost;
    return this;
  }

  public int getBucketSize() {
    return bucketSize;
  }

  public DiscoveryConfiguration setBucketSize(final int bucketSize) {
    this.bucketSize = bucketSize;
    return this;
  }

  @Override
  public boolean equals(final Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof DiscoveryConfiguration)) {
      return false;
    }
    final DiscoveryConfiguration that = (DiscoveryConfiguration) o;
    return active == that.active
        && bindPort == that.bindPort
        && bucketSize == that.bucketSize
        && Objects.equals(bindHost, that.bindHost)
        && Objects.equals(advertisedHost, that.advertisedHost)
        && Objects.equals(bootnodes, that.bootnodes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(active, bindHost, bindPort, advertisedHost, bucketSize, bootnodes);
  }

  @Override
  public String toString() {
    return "DiscoveryConfiguration{"
        + "active="
        + active
        + ", bindHost='"
        + bindHost
        + '\''
        + ", bindPort="
        + bindPort
        + ", advertisedHost='"
        + advertisedHost
        + '\''
        + ", bucketSize="
        + bucketSize
        + ", bootnodes="
        + bootnodes
        + '}';
  }
}
