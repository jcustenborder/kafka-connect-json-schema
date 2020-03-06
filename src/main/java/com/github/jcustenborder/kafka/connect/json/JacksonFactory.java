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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;
import java.text.DecimalFormat;

public class JacksonFactory {

  public static ObjectMapper create() {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper;
  }

  static class SerializationModule extends SimpleModule {
    public SerializationModule() {
      addSerializer(double.class, new DoubleSerializer());
    }
  }

  static class DoubleSerializer extends JsonSerializer<Double> {
    final DecimalFormat decimalFormat;

    public DoubleSerializer() {
      this.decimalFormat = new DecimalFormat("#");
//      this.df.setMaximumFractionDigits(8);
    }


    @Override
    public void serialize(Double aDouble, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
      jsonGenerator.writeRaw(this.decimalFormat.format(aDouble));
    }
  }


}
