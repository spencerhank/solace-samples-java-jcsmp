/*
 * Copyright 2025 Solace Corporation. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.solace.samples.jcsmp.snippets.serdes.avro;

import com.solace.serdes.Deserializer;
import com.solace.serdes.avro.AvroDeserializer;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides code snippets demonstrating the deserialize operation of Avro deserializers
 * with Schema Registry. This class includes scenarios for:
 * <ul>
 *   <li>DeserializeWithUserAvroSchema Deserialize with sample avro User schema</li>
 *   <li>DeserializeWithAvroSchemaReferences Deserialize an avro schema with references to another avro schema</li>
 *   <li>DeserializeWithPrimitiveString Deserialize with avro primitive string type</li>
 *   <li>DeserializeWithPrimitiveInt Deserialize with avro primitive int type</li>
 *   <li>DeserializeWithPrimitiveLong Deserialize with avro primitive long type</li>
 *   <li>DeserializeWithPrimitiveFloat Deserialize with avro primitive float type</li>
 *   <li>DeserializeWithPrimitiveDouble Deserialize with avro primitive double type</li>
 *   <li>DeserializeWithPrimitiveBoolean Deserialize with avro primitive boolean type</li>
 *   <li>DeserializeWithPrimitiveNull Deserialize with avro primitive null object reference</li>
 * </ul>
 */
public class HowToDeserializeWithAvroDeserializer {

    /**
     * Demonstrates how to deserialize with the User avro schema.
     *
     * @param topic Destination string from messaging system.
     * @param payloadBytes serialized User avro object using AvroSerializer.
     * @param headers header map from messaging system.
     */
    public static void DeserializeWithUserAvroSchema(String topic, byte[] payloadBytes, Map<String, Object> headers) throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Create and configure Avro deserializer
        try (Deserializer<GenericRecord> deserializer = new AvroDeserializer<>()) {
            deserializer.configure(config);

            // At this point, the Avro deserializer is configured and ready to use for deserialization.
            // Note the headers map must include the following:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long (required) for schema identification with value that match
            //   the registry content id for user.avsc.
            // - {@link AvroHeaders#AVRO_ENCODING_TYPE} type String (optional) for avro encoded type
            GenericRecord user = deserializer.deserialize(topic, payloadBytes, headers);
            // At this point, the user record can be used in processing.
        }
    }

    /**
     * Demonstrates how to deserialize with schemas that contain references to other schemas.
     * This example shows how to deserialize a UserAccount object that references the User schema.
     *
     * @param topic Destination string from messaging system.
     * @param payloadBytes serialized UserAccount object using AvroSerializer.
     * @param headers header map from messaging system.
     */
    public static void DeserializeWithAvroSchemaReferences(String topic, byte[] payloadBytes, Map<String, Object> headers) throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Create and configure Avro deserializer
        try (Deserializer<GenericRecord> deserializer = new AvroDeserializer<>()) {
            deserializer.configure(config);

            // At this point, the Avro deserializer is configured and ready to use for deserialization.
            // Note the headers map must include the following:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long (required) for schema identification with value that matches
            //   the registry content id for user-account.avsc.
            // - {@link AvroHeaders#AVRO_ENCODING_TYPE} type String (optional) for avro encoded type
            GenericRecord userAccount = deserializer.deserialize(topic, payloadBytes, headers);
            
            // At this point, the userAccount record can be used in processing.
            // You can access the nested User object via:
            // GenericRecord user = (GenericRecord) userAccount.get("user");
        }
    }

    /**
     * Demonstrates how to deserialize with the avro primitive type String.
     *
     * @param topic Destination string from messaging system.
     * @param payloadBytes serialized String object using AvroSerializer.
     * @param headers header map from messaging system.
     */
    public static void DeserializeWithPrimitiveString(String topic, byte[] payloadBytes, Map<String, Object> headers) throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Create and configure Avro deserializer
        try (Deserializer<String> deserializer = new AvroDeserializer<>()) {
            deserializer.configure(config);

            // At this point, the Avro deserializer is configured and ready to use for deserialization.
            // Note the headers map must include the following:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long (required) for schema identification with value that match
            //   the registry content id for primitive schema equivalent to Schema.create(Schema.TYPE.STRING).toString().
            //   For example: {"type": "string"}.
            // - {@link AvroHeaders#AVRO_ENCODING_TYPE} type String (optional) for avro encoded type
            String payload = deserializer.deserialize(topic, payloadBytes, headers);
            // At this point, the payload string can be used in processing.
        }
    }

    /**
     * Demonstrates how to deserialize with the avro primitive type int.
     *
     * @param topic Destination string from messaging system.
     * @param payloadBytes serialized Integer object using AvroSerializer.
     * @param headers header map from messaging system.
     */
    public static void DeserializeWithPrimitiveInt(String topic, byte[] payloadBytes, Map<String, Object> headers) throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Create and configure Avro deserializer
        try (Deserializer<Integer> deserializer = new AvroDeserializer<>()) {
            deserializer.configure(config);

            // At this point, the Avro deserializer is configured and ready to use for deserialization.
            // Note the headers map must include the following:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long (required) for schema identification with value that match
            //   the registry content id for primitive schema equivalent to Schema.create(Schema.TYPE.INT).toString().
            //   For example: {"type": "int"}.
            // - {@link AvroHeaders#AVRO_ENCODING_TYPE} type String (optional) for avro encoded type
            Integer payload = deserializer.deserialize(topic, payloadBytes, headers);
            // At this point, the payload int can be used in processing.
        }
    }

    /**
     * Demonstrates how to deserialize with the avro primitive type long.
     *
     * @param topic Destination string from messaging system.
     * @param payloadBytes serialized Long object using AvroSerializer.
     * @param headers header map from messaging system.
     */
    public static void DeserializeWithPrimitiveLong(String topic, byte[] payloadBytes, Map<String, Object> headers) throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Create and configure Avro deserializer
        try (Deserializer<Long> deserializer = new AvroDeserializer<>()) {
            deserializer.configure(config);

            // At this point, the Avro deserializer is configured and ready to use for deserialization.
            // Note the headers map must include the following:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long (required) for schema identification with value that match
            //   the registry content id for primitive schema equivalent to Schema.create(Schema.TYPE.LONG).toString().
            //   For example: {"type": "long"}.
            // - {@link AvroHeaders#AVRO_ENCODING_TYPE} type String (optional) for avro encoded type
            Long payload = deserializer.deserialize(topic, payloadBytes, headers);
            // At this point, the payload long can be used in processing.
        }
    }

    /**
     * Demonstrates how to deserialize with the avro primitive type float.
     *
     * @param topic Destination string from messaging system.
     * @param payloadBytes serialized Float object using AvroSerializer.
     * @param headers header map from messaging system.
     */
    public static void DeserializeWithPrimitiveFloat(String topic, byte[] payloadBytes, Map<String, Object> headers) throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Create and configure Avro deserializer
        try (Deserializer<Float> deserializer = new AvroDeserializer<>()) {
            deserializer.configure(config);

            // At this point, the Avro deserializer is configured and ready to use for deserialization.
            // Note the headers map must include the following:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long (required) for schema identification with value that match
            //   the registry content id for primitive schema equivalent to Schema.create(Schema.TYPE.FLOAT).toString().
            //   For example: {"type": "float"}.
            // - {@link AvroHeaders#AVRO_ENCODING_TYPE} type String (optional) for avro encoded type
            Float payload = deserializer.deserialize(topic, payloadBytes, headers);
            // At this point, the payload float can be used in processing.
        }
    }

    /**
     * Demonstrates how to deserialize with the avro primitive type double.
     *
     * @param topic Destination string from messaging system.
     * @param payloadBytes serialized Double object using AvroSerializer.
     * @param headers header map from messaging system.
     */
    public static void DeserializeWithPrimitiveDouble(String topic, byte[] payloadBytes, Map<String, Object> headers) throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Create and configure Avro deserializer
        try (Deserializer<Double> deserializer = new AvroDeserializer<>()) {
            deserializer.configure(config);

            // At this point, the Avro deserializer is configured and ready to use for deserialization.
            // Note the headers map must include the following:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long (required) for schema identification with value that match
            //   the registry content id for primitive schema equivalent to Schema.create(Schema.TYPE.DOUBLE).toString().
            //   For example: {"type": "double"}.
            // - {@link AvroHeaders#AVRO_ENCODING_TYPE} type String (optional) for avro encoded type
            Double payload = deserializer.deserialize(topic, payloadBytes, headers);
            // At this point, the payload double can be used in processing.
        }
    }

    /**
     * Demonstrates how to deserialize with the avro primitive type boolean.
     *
     * @param topic Destination string from messaging system.
     * @param payloadBytes serialized Boolean object using AvroSerializer.
     * @param headers header map from messaging system.
     */
    public static void DeserializeWithPrimitiveBoolean(String topic, byte[] payloadBytes, Map<String, Object> headers) throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Create and configure Avro deserializer
        try (Deserializer<Boolean> deserializer = new AvroDeserializer<>()) {
            deserializer.configure(config);

            // At this point, the Avro deserializer is configured and ready to use for deserialization.
            // Note the headers map must include the following:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long (required) for schema identification with value that match
            //   the registry content id for primitive schema equivalent to Schema.create(Schema.TYPE.BOOLEAN).toString().
            //   For example: {"type": "boolean"}.
            // - {@link AvroHeaders#AVRO_ENCODING_TYPE} type String (optional) for avro encoded type
            Boolean payload = deserializer.deserialize(topic, payloadBytes, headers);
            // At this point, the payload boolean can be used in processing.
        }
    }

    /**
     * Demonstrates how to deserialize with the avro primitive null reference.
     *
     * @param topic Destination string from messaging system.
     * @param payloadBytes serialized null object using AvroSerializer.
     * @param headers header map from messaging system.
     */
    public static void DeserializeWithPrimitiveNull(String topic, byte[] payloadBytes, Map<String, Object> headers) throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Create and configure Avro deserializer
        try (Deserializer<Object> deserializer = new AvroDeserializer<>()) {
            deserializer.configure(config);

            // At this point, the Avro deserializer is configured and ready to use for deserialization.
            // Note the headers map must include the following:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long (required) for schema identification with value that match
            //   the registry content id for primitive schema equivalent to Schema.create(Schema.TYPE.NULL).toString().
            //   For example: {"type": "null"}.
            // - {@link AvroHeaders#AVRO_ENCODING_TYPE} type String (optional) for avro encoded type
            Object payload = deserializer.deserialize(topic, payloadBytes, headers);
            // At this point, the payload null can be used in processing.
        }
    }
}
