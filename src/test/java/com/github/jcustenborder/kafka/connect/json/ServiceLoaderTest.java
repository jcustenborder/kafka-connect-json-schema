/**
 * Copyright © 2020 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.json;

import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.ServiceLoader;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ServiceLoaderTest {
  static final Set<String> EXPECTED_TRANSFORMATIONS = new HashSet<>(Arrays.asList(
      "com.github.jcustenborder.kafka.connect.json.FromJson$Key",
      "com.github.jcustenborder.kafka.connect.json.FromJson$Value"
  ));
  static final Set<String> EXPECTED_CONVERTERS = new HashSet<>(Arrays.asList(
      "com.github.jcustenborder.kafka.connect.json.JsonSchemaConverter"
  ));

  @Test
  public void serviceLoaderDiscoversTransformationProviders() {
    Set<String> actual = new HashSet<>();
    for (Transformation<?> transformation : ServiceLoader.load(Transformation.class)) {
      actual.add(transformation.getClass().getName());
    }

    assertTrue(actual.containsAll(EXPECTED_TRANSFORMATIONS), "Missing providers: " + missingProviders(EXPECTED_TRANSFORMATIONS, actual));
    assertFalse(actual.contains("com.github.jcustenborder.kafka.connect.json.FromJson"));
  }

  @Test
  public void serviceLoaderDiscoversConverterProviders() {
    Set<String> actual = new HashSet<>();
    for (Converter converter : ServiceLoader.load(Converter.class)) {
      actual.add(converter.getClass().getName());
    }

    assertTrue(actual.containsAll(EXPECTED_CONVERTERS), "Missing providers: " + missingProviders(EXPECTED_CONVERTERS, actual));
  }

  static Set<String> missingProviders(Set<String> expected, Set<String> actual) {
    Set<String> missing = new HashSet<>(expected);
    missing.removeAll(actual);
    return missing;
  }
}
