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
import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.recommenders.Recommenders;
import com.github.jcustenborder.kafka.connect.utils.config.validators.Validators;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.net.URL;
import java.util.Map;

class FromJsonConfig extends AbstractConfig {
  public static final String SCHEMA_URL_CONF = "json.schema.url";
  static final String SCHEMA_URL_DOC = "Url to retrieve the schema from. Urls can be anything that is " +
      "supported by URL.openStream(). For example the local filesystem file:///schemas/something.json. " +
      "A web address https://www.schemas.com/something.json";
  public static final String SCHEMA_INLINE_CONF = "json.schema.inline";
  static final String SCHEMA_INLINE_DOC = "The JSON schema to use as an escaped string.";
  public static final String SCHEMA_LOCATION_CONF = "json.schema.location";
  static final String SCHEMA_LOCATION_DOC = "Location to retrieve the schema from. " +
      ConfigUtils.enumDescription(SchemaLocation.class);

  public static final String VALIDATE_JSON_ENABLED_CONF = "json.schema.validation.enabled";
  static final String VALIDATE_JSON_ENABLED_DOC = "Flag to determine if the JSON should be validated " +
      "against the schema.";

  public enum SchemaLocation {
    @Description("Loads the schema from the url specified in `" + SCHEMA_URL_CONF + "`.")
    Url,
    @Description("Loads the schema from `" + SCHEMA_INLINE_CONF + "` as an inline string.")
    Inline
  }

  public final URL schemaUrl;
  public final SchemaLocation schemaLocation;
  public final String schemaText;
  public final boolean validateJson;


  public FromJsonConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.schemaUrl = ConfigUtils.url(this, SCHEMA_URL_CONF);
    this.schemaLocation = ConfigUtils.getEnum(SchemaLocation.class, this, SCHEMA_LOCATION_CONF);
    this.schemaText = getString(SCHEMA_INLINE_CONF);
    this.validateJson = getBoolean(VALIDATE_JSON_ENABLED_CONF);
  }

  public static void addConfigItems(ConfigDef configDef) {
    configDef.define(
        ConfigKeyBuilder.of(SCHEMA_URL_CONF, ConfigDef.Type.STRING)
            .documentation(SCHEMA_URL_DOC)
            .validator(Validators.validUrl())
            .importance(ConfigDef.Importance.HIGH)
            .recommender(Recommenders.visibleIf(SCHEMA_LOCATION_CONF, SchemaLocation.Url.toString()))
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
        ConfigKeyBuilder.of(VALIDATE_JSON_ENABLED_CONF, ConfigDef.Type.BOOLEAN)
            .documentation(VALIDATE_JSON_ENABLED_DOC)
            .importance(ConfigDef.Importance.MEDIUM)
            .defaultValue(false)
            .build()
    ).define(
        ConfigKeyBuilder.of(SCHEMA_INLINE_CONF, ConfigDef.Type.STRING)
            .documentation(SCHEMA_INLINE_DOC)
            .recommender(Recommenders.visibleIf(SCHEMA_LOCATION_CONF, SchemaLocation.Inline.toString()))
            .importance(ConfigDef.Importance.HIGH)
            .defaultValue("")
            .build());
  }

  public static ConfigDef config() {
    ConfigDef configDef = new ConfigDef();
    addConfigItems(configDef);
    return configDef;
  }
}
