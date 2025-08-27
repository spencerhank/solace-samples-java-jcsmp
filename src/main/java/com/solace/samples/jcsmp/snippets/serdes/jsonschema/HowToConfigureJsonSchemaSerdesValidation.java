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
import com.solace.serdes.Deserializer;
import com.solace.serdes.Serializer;
import com.solace.serdes.jsonschema.JsonSchemaDeserializer;
import com.solace.serdes.jsonschema.JsonSchemaProperties;
import com.solace.serdes.jsonschema.JsonSchemaSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides code snippets demonstrating how to configure JSON Schema SERDES validation.
 * This class includes scenarios for:
 * <ul>
 *   <li>Enabling validation on the JsonSchemaSerializer</li>
 *   <li>Enabling validation on the JsonSchemaDeserializer</li>
 * </ul>
 */
public class HowToConfigureJsonSchemaSerdesValidation {

    /**
     * Demonstrates how to enable JSON schema validation on the JsonSchemaSerializer.
     */
    public static void enableSerializerValidation() throws IOException {
        Map<String, Object> config = new HashMap<>();
        // Enable JSON schema validation during serialization.
        config.put(JsonSchemaProperties.VALIDATE_SCHEMA, true);

        try (Serializer<JsonNode> serializer = new JsonSchemaSerializer<>()) {
            serializer.configure(config);
            // JsonSchemaSerializer is now configured to validate JSON schemas.
        }
    }

    /**
     * Demonstrates how to enable JSON schema validation on the JsonSchemaDeserializer.
     */
    public static void enableDeserializerValidation() throws IOException {
        Map<String, Object> config = new HashMap<>();
        // Enable JSON schema validation during deserialization.
        config.put(JsonSchemaProperties.VALIDATE_SCHEMA, true);

        try (Deserializer<JsonNode> deserializer = new JsonSchemaDeserializer<>()) {
            deserializer.configure(config);
            // JsonSchemaDeserializer is now configured to validate JSON schemas.
        }
    }
}
