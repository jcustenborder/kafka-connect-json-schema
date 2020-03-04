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

import com.github.jcustenborder.kafka.connect.utils.transformation.BaseKeyValueTransformation;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;

import java.util.Map;

public class FromJson<R extends ConnectRecord<R>> extends BaseKeyValueTransformation<R> {
  FromJsonConfig config;

  protected FromJson(boolean isKey) {
    super(isKey);
  }

  @Override
  public ConfigDef config() {
    return FromJsonConfig.config();
  }

  @Override
  public void close() {

  }

  @Override
  protected SchemaAndValue processBytes(R record, Schema inputSchema, byte[] input) {
    return super.processBytes(record, inputSchema, input);
  }

  @Override
  protected SchemaAndValue processString(R record, Schema inputSchema, String input) {
    return super.processString(record, inputSchema, input);
  }

  @Override
  public void configure(Map<String, ?> map) {
    this.config = new FromJsonConfig(map);
  }

  public static class Key extends FromJson {
    public Key() {
      super(true);
    }
  }

  public static class Value extends FromJson {
    public Value() {
      super(false);
    }
  }
}
