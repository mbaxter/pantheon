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
package tech.pegasys.pantheon.cli;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;
import static tech.pegasys.pantheon.cli.DefaultCommandValues.MANDATORY_FILE_FORMAT_HELP;
import static tech.pegasys.pantheon.cli.PublicKeySubCommand.COMMAND_NAME;

import tech.pegasys.pantheon.cli.PublicKeySubCommand.AddressSubCommand;
import tech.pegasys.pantheon.cli.PublicKeySubCommand.ExportSubCommand;
import tech.pegasys.pantheon.controller.PantheonController;
import tech.pegasys.pantheon.crypto.SECP256K1.KeyPair;
import tech.pegasys.pantheon.ethereum.core.Address;
import tech.pegasys.pantheon.ethereum.core.Util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

/** Node's public key related sub-command */
@Command(
    name = COMMAND_NAME,
    description = "This command provides node public key related actions.",
    mixinStandardHelpOptions = true,
    subcommands = {ExportSubCommand.class, AddressSubCommand.class})
class PublicKeySubCommand implements Runnable {
  private static final Logger LOG = LogManager.getLogger();

  static final String COMMAND_NAME = "public-key";

  @SuppressWarnings("unused")
  @ParentCommand
  private PantheonCommand parentCommand; // Picocli injects reference to parent command

  @SuppressWarnings("unused")
  @Spec
  private CommandSpec spec; // Picocli injects reference to command spec

  private final PrintStream out;

  PublicKeySubCommand(final PrintStream out) {
    this.out = out;
  }

  @Override
  public void run() {
    spec.commandLine().usage(out);
  }

  /**
   * Public key export sub-command
   *
   * <p>Export of the public key is writing the key to the standard output by default. An option
   * enables to write it in a file. Indeed, a direct output of the value to standard out is not
   * always recommended as reading can be made difficult as the value can be mixed with other
   * information like logs that are in KeyPairUtil that is inevitable.
   */
  @Command(
      name = "export",
      description = "This command outputs the node public key. Default output is standard output.",
      mixinStandardHelpOptions = true)
  static class ExportSubCommand implements Runnable {

    @Option(
        names = "--to",
        paramLabel = MANDATORY_FILE_FORMAT_HELP,
        description = "File to write public key to instead of standard output",
        arity = "1..1")
    private File publicKeyExportFile = null;

    @SuppressWarnings("unused")
    @ParentCommand
    private PublicKeySubCommand parentCommand; // Picocli injects reference to parent command

    @Override
    public void run() {
      checkNotNull(parentCommand);
      checkNotNull(parentCommand.parentCommand);

      final PantheonController<?> controller = parentCommand.parentCommand.buildController();
      final KeyPair keyPair = controller.getLocalNodeKeyPair();

      // if we have an output file defined, print to it
      // otherwise print to standard output.
      if (publicKeyExportFile != null) {
        final Path path = publicKeyExportFile.toPath();

        try (final BufferedWriter fileWriter = Files.newBufferedWriter(path, UTF_8)) {
          fileWriter.write(keyPair.getPublicKey().toString());
        } catch (final IOException e) {
          LOG.error("An error occurred while trying to write the public key", e);
        }
      } else {
        parentCommand.out.println(keyPair.getPublicKey().toString());
      }
    }
  }

  /**
   * Public key address export sub-command
   *
   * <p>Export of the public key address is writing the address to the standard output by default.
   * An option enables to write it in a file. Indeed, a direct output of the value to standard out
   * is not always recommended as reading can be made difficult as the value can be mixed with other
   * information like logs that are in KeyPairUtil that is inevitable.
   */
  @Command(
      name = "export-address",
      description =
          "This command outputs the node's public key address. "
              + "Default output is standard output.",
      mixinStandardHelpOptions = true)
  static class AddressSubCommand implements Runnable {

    @Option(
        names = "--to",
        paramLabel = MANDATORY_FILE_FORMAT_HELP,
        description = "File to write address to instead of standard output",
        arity = "1..1")
    private File addressExportFile = null;

    @SuppressWarnings("unused")
    @ParentCommand
    private PublicKeySubCommand parentCommand; // Picocli injects reference to parent command

    @Override
    public void run() {
      checkNotNull(parentCommand);
      checkNotNull(parentCommand.parentCommand);

      final PantheonController<?> controller = parentCommand.parentCommand.buildController();
      final KeyPair keyPair = controller.getLocalNodeKeyPair();
      final Address address = Util.publicKeyToAddress(keyPair.getPublicKey());

      // if we have an output file defined, print to it
      // otherwise print to standard output.
      if (addressExportFile != null) {
        final Path path = addressExportFile.toPath();

        try (final BufferedWriter fileWriter = Files.newBufferedWriter(path, UTF_8)) {
          fileWriter.write(address.toString());
        } catch (final IOException e) {
          LOG.error("An error occurred while trying to write the public key address", e);
        }
      } else {
        parentCommand.out.println(address.toString());
      }
    }
  }
}
