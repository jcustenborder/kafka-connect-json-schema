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

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import com.github.jcustenborder.kafka.connect.utils.config.recommenders.Recommenders;
import com.github.jcustenborder.kafka.connect.utils.config.validators.Validators;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.net.URL;
import java.util.Map;

class FromJsonConfig extends AbstractConfig {
  public static final String SCHEMA_URL_CONF = "json.schema.url";
  static final String SCHEMA_URL_DOC = "Url to retrieve the schema from.";
  public static final String SCHEMA_INLINE_CONF = "json.schema.inline";
  static final String SCHEMA_INLINE_DOC = "The JSON schema to use as an escaped string.";
  public static final String SCHEMA_LOCATION_CONF = "json.schema.location";
  static final String SCHEMA_LOCATION_DOC = "Location to retrieve the schema from. `Url` is used " +
      "to retrieve the schema from a url. `Inline` is used to read the schema from the `" +
      SCHEMA_INLINE_CONF + "` configuration value.";

  public enum SchemaLocation {
    Url,
    Inline
  }

  public final URL schemaUrl;
  public final SchemaLocation schemaLocation;
  public final String schemaText;


  public FromJsonConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.schemaUrl = ConfigUtils.url(this, SCHEMA_URL_CONF);
    this.schemaLocation = ConfigUtils.getEnum(SchemaLocation.class, this, SCHEMA_LOCATION_CONF);
    this.schemaText = getString(SCHEMA_INLINE_CONF);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(
            ConfigKeyBuilder.of(SCHEMA_URL_CONF, ConfigDef.Type.STRING)
                .documentation(SCHEMA_URL_DOC)
                .validator(Validators.validUrl())
                .importance(ConfigDef.Importance.HIGH)
                .defaultValue("File:///doesNotExist")
                .build()
        ).define(
            ConfigKeyBuilder.of(SCHEMA_LOCATION_CONF, ConfigDef.Type.STRING)
                .documentation(SCHEMA_LOCATION_DOC)
                .validator(Validators.validEnum(SchemaLocation.class))
                .recommender(Recommenders.enumValues(SchemaLocation.class))
                .importance(ConfigDef.Importance.HIGH)
                .defaultValue(SchemaLocation.Url.toString())
                .build()
        ).define(
            ConfigKeyBuilder.of(SCHEMA_INLINE_CONF, ConfigDef.Type.STRING)
                .documentation(SCHEMA_INLINE_DOC)
                .recommender(Recommenders.visibleIf(SCHEMA_LOCATION_CONF, SchemaLocation.Inline.toString()))
                .importance(ConfigDef.Importance.HIGH)
                .defaultValue("")
                .build()
        );
  }
}
