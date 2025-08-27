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
import com.solace.serdes.common.resolver.config.SchemaResolverProperties;
import com.solace.serdes.jsonschema.JsonSchemaDeserializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides code snippets demonstrating the configuration of JSON Schema deserializers
 * with Schema Registry connections. This class includes scenarios for:
 * <ul>
 *   <li>Non-authenticated connections</li>
 *   <li>Authenticated connections (basic auth)</li>
 *   <li>Secure connections using TLS (includes basic authentication and no authentication)</li>
 * </ul>
 */
public class HowToConfigureJsonSchemaDeserializerSchemaRegistryConnection {

    /**
     * Demonstrates how to configure a JSON Schema deserializer with a Schema Registry endpoint without authentication.
     */
    public static void DeserializeWithSchemaRegistryEndpoint() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set Schema Registry URL
        config.put(SchemaResolverProperties.REGISTRY_URL, "http://localhost:8081/apis/registry/v3");

        // Create and configure JSON Schema deserializer
        try (Deserializer<JsonNode> deserializer = new JsonSchemaDeserializer<>()) {
            deserializer.configure(config);

            // At this point, the JSON Schema deserializer is configured and ready to use for deserialization.
        }
    }

    /**
     * Demonstrates how to configure a JSON Schema deserializer with an authenticated Schema Registry endpoint.
     */
    public static void DeserializeWithAuthenticatedSchemaRegistryEndpoint() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set Schema Registry URL
        config.put(SchemaResolverProperties.REGISTRY_URL, "http://localhost:8081/apis/registry/v3");

        // Set authentication credentials
        config.put(SchemaResolverProperties.AUTH_USERNAME, "sr-readonly");
        config.put(SchemaResolverProperties.AUTH_PASSWORD, "roPassword");

        // Create and configure JSON Schema deserializer
        try (Deserializer<JsonNode> deserializer = new JsonSchemaDeserializer<>()) {
            deserializer.configure(config);

            // At this point, the JSON Schema deserializer is configured and ready to use for deserialization.
        }
    }

    /**
     * Demonstrates how to configure a JSON Schema deserializer with a Secure Schema Registry endpoint without authentication.
     */
    public static void DeserializeWithSecureSchemaRegistryEndpoint() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set Schema Registry URL
        // NOTE: Use HTTPS for secure communication with the Schema Registry
        config.put(SchemaResolverProperties.REGISTRY_URL, "https://localhost:8081/apis/registry/v3");

        // Configure TLS properties for secure connection
        // This includes the truststore path and password
        config.put(SchemaResolverProperties.TRUST_STORE_PATH, "path/to/truststore");
        config.put(SchemaResolverProperties.TRUST_STORE_PASSWORD, "truststore_password");

        // Configure certificate validation (by default set to true)
        // NOTE: Disabling certificate validation can be useful for debugging purposes,
        // but it's not recommended for production use as it reduces security.
        // config.put(SchemaResolverProperties.VALIDATE_CERTIFICATE, false);

        // Create and configure JSON Schema deserializer
        try (Deserializer<JsonNode> deserializer = new JsonSchemaDeserializer<>()) {
            deserializer.configure(config);

            // At this point, the JSON Schema deserializer is configured and ready to use for deserialization.
        }
    }

    /**
     * Demonstrates how to configure a JSON Schema deserializer with an authenticated Secure Schema Registry endpoint.
     */
    public static void DeserializeWithAuthenticatedSecureSchemaRegistryEndpoint() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set Schema Registry URL
        // NOTE: Use HTTPS for secure communication with the Schema Registry
        config.put(SchemaResolverProperties.REGISTRY_URL, "https://localhost:8081/apis/registry/v3");

        // Set authentication credentials
        config.put(SchemaResolverProperties.AUTH_USERNAME, "sr-readonly");
        config.put(SchemaResolverProperties.AUTH_PASSWORD, "roPassword");

        // Configure TLS properties for secure connection
        // This includes the truststore path and password
        config.put(SchemaResolverProperties.TRUST_STORE_PATH, "path/to/truststore");
        config.put(SchemaResolverProperties.TRUST_STORE_PASSWORD, "truststore_password");

        // Configure certificate validation (by default set to true)
        // NOTE: Disabling certificate validation can be useful for debugging purposes,
        // but it's not recommended for production use as it reduces security.
        // config.put(SchemaResolverProperties.VALIDATE_CERTIFICATE, false);

        // Create and configure JSON Schema deserializer
        try (Deserializer<JsonNode> deserializer = new JsonSchemaDeserializer<>()) {
            deserializer.configure(config);

            // At this point, the JSON Schema deserializer is configured and ready to use for deserialization.
        }
    }
}
