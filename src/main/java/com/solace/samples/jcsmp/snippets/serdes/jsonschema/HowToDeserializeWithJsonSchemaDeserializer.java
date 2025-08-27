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

package com.solace.samples.jcsmp.snippets.serdes.jsonschema;

import com.fasterxml.jackson.databind.JsonNode;
import com.solace.samples.serdes.jsonschema.User;
import com.solace.samples.serdes.jsonschema.UserAccount;
import com.solace.serdes.Deserializer;
import com.solace.serdes.common.SerdeProperties;
import com.solace.serdes.jsonschema.JsonSchemaDeserializer;
import com.solace.serdes.jsonschema.JsonSchemaProperties;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides a code snippet demonstrating the deserialize operation of a JSON Schema deserializer
 * with Schema Registry. This class includes scenarios for:
 * <ul>
 *   <li>DeserializeToPojoWithDeserializedType Deserialize to a POJO using the deserialized type configuration.</li>
 *   <li>DeserializeToPojoWithTypeProperty Deserialize to a POJO using the 'customJavaType' property in the schema.</li>
 *   <li>DeserializeToJsonNode Deserialize to a JsonNode when no type information is provided.</li>
 *   <li>DeserializeWithJsonSchemaReferences Deserialize a JSON schema with references to another JSON schema.</li>
 * </ul>
 */
public class HowToDeserializeWithJsonSchemaDeserializer {

    /**
     * Demonstrates how to deserialize to a POJO using the {@link SerdeProperties#DESERIALIZED_TYPE} configuration.
     * This property takes precedence over the {@link JsonSchemaProperties#TYPE_PROPERTY} if both are present.
     *
     * @param topic        Destination string from the messaging system.
     * @param payloadBytes Serialized User JSON object using JsonSchemaSerializer.
     * @param headers      Header map from the messaging system.
     */
    public static void DeserializeToPojoWithDeserializedType(String topic, byte[] payloadBytes, Map<String, Object> headers) throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Set the deserialized type to the User class
        config.put(SerdeProperties.DESERIALIZED_TYPE, "com.solace.samples.serdes.jsonschema.User");

        // Create and configure JSON Schema deserializer
        try (Deserializer<User> deserializer = new JsonSchemaDeserializer<>()) {
            deserializer.configure(config);

            // At this point, the JSON Schema deserializer is configured and ready to use for deserialization.
            // Note the headers map must include the following:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long (required) for schema identification with a value that matches
            //   the registry content id for user.json.
            User user = deserializer.deserialize(topic, payloadBytes, headers);
            // At this point, the user object can be used in processing.
        }
    }

    /**
     * Demonstrates how to deserialize to a POJO using the 'customJavaType' property within the User JSON schema.
     * The deserializer uses this property to determine the target Java type.
     *
     * @param topic        Destination string from the messaging system.
     * @param payloadBytes Serialized User JSON object using JsonSchemaSerializer.
     * @param headers      Header map from the messaging system.
     */
    public static void DeserializeToPojoWithTypeProperty(String topic, byte[] payloadBytes, Map<String, Object> headers) throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // The type property is used to determine the Java type. This can be changed by setting the TYPE_PROPERTY config.
        // The default value is "javaType".
        config.put(JsonSchemaProperties.TYPE_PROPERTY, "customJavaType");

        // Create and configure JSON Schema deserializer
        try (Deserializer<User> deserializer = new JsonSchemaDeserializer<>()) {
            deserializer.configure(config);

            // At this point, the JSON Schema deserializer is configured and ready to use for deserialization.
            // Note the headers map must include the following:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long (required) for schema identification with a value that matches
            //   the registry content id for user.json.
            User user = deserializer.deserialize(topic, payloadBytes, headers);
            // At this point, the user object can be used in processing.
        }
    }

    /**
     * Demonstrates how to deserialize to a {@link JsonNode} as a fallback. When neither the
     * {@link SerdeProperties#DESERIALIZED_TYPE} configuration nor a type property (e.g., 'javaType')
     * is present in the JSON schema, the payload is deserialized to a generic {@link JsonNode}.
     *
     * @param topic        Destination string from the messaging system.
     * @param payloadBytes Serialized JSON object using JsonSchemaSerializer.
     * @param headers      Header map from the messaging system.
     */
    public static void DeserializeToJsonNode(String topic, byte[] payloadBytes, Map<String, Object> headers) throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Create and configure JSON Schema deserializer
        try (Deserializer<JsonNode> deserializer = new JsonSchemaDeserializer<>()) {
            deserializer.configure(config);

            // At this point, the JSON Schema deserializer is configured and ready to use for deserialization.
            // When no type information is provided, the deserializer returns a JsonNode.
            // Note the headers map must include the following:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long (required) for schema identification with a value that matches
            //   the registry content id for the schema.
            JsonNode jsonNode = deserializer.deserialize(topic, payloadBytes, headers);
            // At this point, the jsonNode object can be used in processing.
        }
    }

    /**
     * Demonstrates how to deserialize with schemas that contain references to other schemas.
     * This example shows how to deserialize a UserAccount object that references the User schema.
     *
     * @param topic        Destination string from messaging system.
     * @param payloadBytes Serialized UserAccount JSON object using JsonSchemaSerializer.
     * @param headers      Header map from messaging system.
     */
    public static void DeserializeWithJsonSchemaReferences(String topic, byte[] payloadBytes, Map<String, Object> headers) throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Set the deserialized type to the UserAccount class
        config.put(SerdeProperties.DESERIALIZED_TYPE, UserAccount.class);

        // NOTE: When JSON Schema Validation is enabled, the value of $ref must be a valid URI that contains the schema reference name in it.
        // JSON Schema Validation is enabled by default.
        // config.put(JsonSchemaProperties.VALIDATE_SCHEMA, false);

        // Create and configure JSON Schema deserializer
        try (Deserializer<UserAccount> deserializer = new JsonSchemaDeserializer<>()) {
            deserializer.configure(config);

            // At this point, the JSON Schema deserializer is configured and ready to use for deserialization.
            // Note the headers map must include the following:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long (required) for schema identification with a value that matches
            //   the registry content id for user-account.json.
            UserAccount userAccount = deserializer.deserialize(topic, payloadBytes, headers);

            // At this point, the userAccount object can be used in processing.
            // You can access the nested User object via:
            User user = userAccount.getUser();
        }
    }
}
