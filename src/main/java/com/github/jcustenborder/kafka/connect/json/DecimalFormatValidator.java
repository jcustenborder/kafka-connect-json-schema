package com.github.jcustenborder.kafka.connect.json;

import org.everit.json.schema.FormatValidator;

import java.util.Optional;

class DecimalFormatValidator implements FormatValidator {
  @Override
  public Optional<String> validate(String s) {
    return Optional.empty();
  }

  @Override
  public String formatName() {
    return "decimal";
  }
}
