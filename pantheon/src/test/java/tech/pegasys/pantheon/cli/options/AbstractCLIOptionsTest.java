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
package tech.pegasys.pantheon.cli.options;

import static org.assertj.core.api.Assertions.assertThat;

import tech.pegasys.pantheon.cli.CommandTestAbstract;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

public abstract class AbstractCLIOptionsTest<D, T extends CLIOptions<?>>
    extends CommandTestAbstract {
  @Test
  public void fromDomainObject_default() {
    fromDomainObject(createDefaultDomainObject());
  }

  @Test
  public void fromDomainObject_customize() {
    fromDomainObject(createCustomizedDomainObject());
  }

  private void fromDomainObject(final D domainObject) {
    final T options = optionsFromDomainObject(domainObject);
    final D domainObjectFromOptions = optionsToDomainObject(options);

    final List<String> fieldsToIgnore = getFieldsToIgnore();
    final String[] ignored = fieldsToIgnore.toArray(new String[0]);
    assertThat(domainObjectFromOptions).isEqualToIgnoringGivenFields(domainObject, ignored);
  }

  @Test
  public void getCLIOptions_default() {
    getCLIOptions(createDefaultDomainObject());
  }

  @Test
  public void getCLIOptions_custom() {
    getCLIOptions(createCustomizedDomainObject());
  }

  private void getCLIOptions(final D domainObject) {
    T options = optionsFromDomainObject(domainObject);
    final String[] cliOptions = options.getCLIOptions().toArray(new String[0]);

    parseCommand(cliOptions);
    final D actualDomainObject = getDomainObjectFromPantheonCommand();

    final List<String> fieldsToIgnore = getFieldsToIgnore();
    final String[] ignored = fieldsToIgnore.toArray(new String[0]);
    assertThat(actualDomainObject).isEqualToIgnoringGivenFields(domainObject, ignored);

    assertThat(commandOutput.toString()).isEmpty();
    assertThat(commandErrorOutput.toString()).isEmpty();
  }

  @Test
  public void defaultValues() {
    parseCommand();

    final D defaultDomainObject = createDefaultDomainObject();
    final D actualDomainObject = getDomainObjectFromPantheonCommand();

    // Check default values supplied by CLI match expected default values
    final List<String> fieldsToIgnore = new ArrayList<>();
    fieldsToIgnore.addAll(getFieldsToIgnore());
    fieldsToIgnore.addAll(getFieldsWithComputedDefaults());
    final String[] ignoredDefaults = fieldsToIgnore.toArray(new String[0]);
    assertThat(actualDomainObject)
        .isEqualToIgnoringGivenFields(defaultDomainObject, ignoredDefaults);
  }

  abstract D createDefaultDomainObject();

  abstract D createCustomizedDomainObject();

  protected List<String> getFieldsWithComputedDefaults() {
    return Collections.emptyList();
  }

  protected List<String> getFieldsToIgnore() {
    return Collections.emptyList();
  }

  abstract T optionsFromDomainObject(D domainObject);

  abstract D optionsToDomainObject(T options);

  abstract D getDomainObjectFromPantheonCommand();
}