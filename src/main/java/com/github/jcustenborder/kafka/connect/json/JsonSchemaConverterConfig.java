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
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

class JsonSchemaConverterConfig extends JsonConfig {
  public final boolean insertSchema;

  public final static String INSERT_SCHEMA_ENABLED_CONF = "json.insert.schema.enabled";
  final static String INSERT_SCHEMA_ENABLED_DOC = "Flag to determine if the schema specified should be " +
      "used if there is no schema header found. This allows a connector to consume a topic " +
      "that does not have schema headers and apply an external header.";

  public JsonSchemaConverterConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.insertSchema = getBoolean(INSERT_SCHEMA_ENABLED_CONF);
  }

  public static ConfigDef config() {
    return JsonConfig.config()
        .define(
            ConfigKeyBuilder.of(INSERT_SCHEMA_ENABLED_CONF, ConfigDef.Type.BOOLEAN)
                .documentation(INSERT_SCHEMA_ENABLED_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .defaultValue(false)
                .build()
        );
  }
}
