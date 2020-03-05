package com.github.jcustenborder.kafka.connect.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;
import java.text.DecimalFormat;

public class JacksonFactory {

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

  public static ObjectMapper create() {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper;
  }


}
