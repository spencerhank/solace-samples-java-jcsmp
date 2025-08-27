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

import com.solace.samples.serdes.jsonschema.User;
import com.solace.samples.serdes.jsonschema.UserAccount;
import com.solace.serdes.Serializer;
import com.solace.serdes.common.resolver.config.SchemaResolverProperties;
import com.solace.serdes.jsonschema.JsonSchemaSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides code snippets demonstrating the serializer operation of JSON Schema serializers
 * with Schema Registry resolution configurations. This class includes scenarios for:
 * <ul>
 *   <li>SerializeWithUserJsonSchema Serialize with sample JSON User schema</li>
 *   <li>SerializeWithFindLatest Serialize with schema resolver 'find-latest' option</li>
 *   <li>SerializeWithJsonSchemaReferences Serialize a JSON schema with references to another JSON schema</li>
 *   <li>SerializeWithExplicitSchemaVersion Serialize a JSON User schema with an explicit version from the schema registry</li>
 * </ul>
 */
public class HowToSerializeWithJsonSchemaSerializer {

    /**
     * Demonstrates how to serialize with the User JSON schema.
     */
    public static void SerializeWithUserJsonSchema() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // create User JSON object
        User user = new User();
        user.setName("John Doe");
        user.setId("-1");
        user.setEmail("support@solace.com");

        // Create and configure JSON Schema serializer
        try (Serializer<User> serializer = new JsonSchemaSerializer<>()) {
            serializer.configure(config);
            // create headers map for serialization
            Map<String, Object> headers = new HashMap<>();

            // At this point, the JSON Schema serializer is configured and ready to use for serialization.
            byte[] userBytes = serializer.serialize("solace/samples/json", user, headers);

            // at this point, userBytes and headers are ready to be applied to the messaging system of choice
            // userBytes are the user object serialized as bytes
            // headers were modified to hold the schema registry header fields to include:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long for schema identification
        }
    }

    /**
     * Demonstrates how to serialize with using the latest schema from the registry.
     */
    public static void SerializeWithFindLatest() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // add property to resolve the latest schema from the registry
        config.put(SchemaResolverProperties.FIND_LATEST_ARTIFACT, true);

        // create User JSON object
        User user = new User();
        user.setName("John Doe");
        user.setId("-1");
        user.setEmail("support@solace.com");

        // Create and configure JSON Schema serializer
        try (Serializer<User> serializer = new JsonSchemaSerializer<>()) {
            serializer.configure(config);
            // create headers map for serialization
            Map<String, Object> headers = new HashMap<>();

            // At this point, the JSON Schema serializer is configured and ready to use for serialization.
            // The Serializer serialize will pull the latest version of the schema to resolve even if the resolver strategy
            // can load the schema.
            byte[] userBytes = serializer.serialize("solace/samples/json", user, headers);

            // at this point, userBytes and headers are ready to be applied to the messaging system of choice
            // userBytes are the user object serialized as bytes
            // headers were modified to hold the schema registry header fields to include:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long for schema identification
        }
    }

    /**
     * Demonstrates how to serialize a JSON schema with references to another JSON schema.
     * Note: When working with schema references, the referenced schemas (e.g., User) must be
     * uploaded to the registry before the schemas that reference them (e.g., UserAccount).
     */
    public static void SerializeWithJsonSchemaReferences() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // NOTE: When JSON Schema Validation is enabled, the value of $ref must be a valid URI that contains the schema reference name in it.
        // JSON Schema Validation is enabled by default.
        // config.put(JsonSchemaProperties.VALIDATE_SCHEMA, false);

        // Create UserAccount JSON object with nested User object
        UserAccount userAccount = new UserAccount();
        userAccount.setAccountId("-1");
        userAccount.setIsActive(true);

        // Create and set the nested User record
        User user = new User();
        user.setName("John Doe");
        user.setId("-1");
        user.setEmail("support@solace.com");
        userAccount.setUser(user);

        // Create and configure JSON Schema serializer
        try (Serializer<UserAccount> serializer = new JsonSchemaSerializer<>()) {
            serializer.configure(config);
            // create headers map for serialization
            Map<String, Object> headers = new HashMap<>();

            // At this point, the JSON Schema serializer is configured and ready to use for serialization.
            byte[] userAccountBytes = serializer.serialize("solace/samples/user-account/json", userAccount, headers);

            // at this point, userAccountBytes and headers are ready to be applied to the messaging system of choice
            // userAccountBytes are the userAccount object serialized as bytes
            // headers were modified to hold the schema registry header fields to include:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long for schema identification
        }
    }

    /**
     * Demonstrates how to serialize a JSON User schema with an explicit version from the schema registry
     */
    public static void SerializeWithExplicitSchemaVersion() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // add property to resolve an explicit version of schema from the registry
        config.put(SchemaResolverProperties.EXPLICIT_ARTIFACT_VERSION, "1");

        // create User JSON object
        User user = new User();
        user.setName("John Doe");
        user.setId("-1");
        user.setEmail("support@solace.com");

        // Create and configure JSON Schema serializer
        try (Serializer<User> serializer = new JsonSchemaSerializer<>()) {
            serializer.configure(config);
            // create headers map for serialization
            Map<String, Object> headers = new HashMap<>();

            // At this point, the JSON Schema serializer is configured and ready to use for serialization.
            // The call to serialize will pull the explicit version of the schema to resolve even if the resolver strategy
            // can load the schema.
            byte[] userBytes = serializer.serialize("solace/samples/json", user, headers);

            // at this point, userBytes and headers are ready to be applied to the messaging system of choice
            // userBytes are the user object serialized as bytes
            // headers were modified to hold the schema registry header fields to include:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long for schema identification
        }
    }
}
