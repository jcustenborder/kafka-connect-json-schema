# Introduction
[Documentation](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-json-schema)

Installation through the [Confluent Hub Client](https://docs.confluent.io/current/connect/managing/confluent-hub/client.html)

This plugin is used to add additional JSON parsing functionality to Kafka Connect.

## [From Json transformation](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-json-schema/transformations/FromJson.html)

The FromJson will read JSON data that is in string on byte form and parse the data to a connect structure based on the JSON schema provided.

# Development

## Building the source

```bash
mvn clean package
```