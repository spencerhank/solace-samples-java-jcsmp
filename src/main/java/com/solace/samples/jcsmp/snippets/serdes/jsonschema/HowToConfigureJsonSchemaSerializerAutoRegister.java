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
import com.solace.serdes.SerializationException;
import com.solace.serdes.Serializer;
import com.solace.serdes.common.resolver.config.SchemaResolverProperties;
import com.solace.serdes.jsonschema.JsonSchemaSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides code snippets demonstrating how to configure JSON Schema serializers with auto-registration properties.
 * This class includes scenarios for:
 * <ul>
 *   <li>Auto-registration with different IfArtifactExists options: CREATE_VERSION, FAIL, FIND_OR_CREATE_VERSION</li>
 * </ul>
 */
public class HowToConfigureJsonSchemaSerializerAutoRegister {

    /**
     * Demonstrates how to configure a JSON Schema serializer with auto-registration enabled
     * and the IfArtifactExists option set to CREATE_VERSION.
     * 
     * <p>When auto-registration is enabled with CREATE_VERSION, the serializer will automatically
     * register schemas that don't exist in the registry during serialization. If a schema with the same
     * content already exists in the registry, it will create a new version of that schema.</p>
     */
    public static void ConfigureAutoRegisterWithCreateVersionIfExists() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Enable auto-registration of schemas
        config.put(SchemaResolverProperties.AUTO_REGISTER_ARTIFACT, true);
        // Specifies the classpath resource path to the schema to be uploaded by auto-register.
        config.put(SchemaResolverProperties.SCHEMA_LOCATION, "json-schema/user.json");
        
        // Set behavior when artifact already exists to CREATE_VERSION
        config.put(SchemaResolverProperties.AUTO_REGISTER_ARTIFACT_IF_EXISTS, 
                SchemaResolverProperties.IfArtifactExists.CREATE_VERSION);

        // Create and configure JSON Schema serializer
        try (Serializer<User> serializer = new JsonSchemaSerializer<>()) {
            serializer.configure(config);

            // At this point, the JSON Schema serializer is configured and ready to use for serialization.
            // When serializing data with a schema that doesn't exist in the registry:
            // - The schema will be automatically registered in the registry
            // - If a schema with the same name already exists, a new version will be created
        }
    }

    /**
     * Demonstrates how to configure a JSON Schema serializer with auto-registration enabled
     * and the IfArtifactExists option set to FAIL.
     * 
     * <p>When auto-registration is enabled with FAIL, the serializer will automatically
     * register schemas that don't exist in the registry during serialization. However, if a schema with the same
     * content already exists in the registry, the registration will fail and throw a {@link SerializationException}.</p>
     *
     * @param objectToSerialize The JSON Schema {@link User} to serialize
     * @param headers Message headers to use for the serialization
     */
    public static void ConfigureAutoRegisterWithFailIfExists(User objectToSerialize, Map<String, Object> headers) throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Enable auto-registration of schemas
        config.put(SchemaResolverProperties.AUTO_REGISTER_ARTIFACT, true);
        // Specifies the classpath resource path to the schema to be uploaded by auto-register.
        config.put(SchemaResolverProperties.SCHEMA_LOCATION, "json-schema/user.json");
        
        // Set behavior when artifact already exists to FAIL
        config.put(SchemaResolverProperties.AUTO_REGISTER_ARTIFACT_IF_EXISTS, 
                SchemaResolverProperties.IfArtifactExists.FAIL);

        // Create and configure JSON Schema serializer
        try (Serializer<User> serializer = new JsonSchemaSerializer<>()) {
            serializer.configure(config);

            // At this point, the JSON Schema serializer is configured and ready to use for serialization.

            try {
                // When serializing data with a schema that doesn't exist in the registry:
                // - The schema will be automatically registered in the registry
                byte[] serializedBytes = serializer.serialize("solace/samples/jsonschema", objectToSerialize, headers);
            } catch (SerializationException e) {
                // If a schema with the same name already exists, registration will fail with a SerializationException
            }
        }
    }

    /**
     * Demonstrates how to configure a JSON Schema serializer with auto-registration enabled
     * and the IfArtifactExists option set to FIND_OR_CREATE_VERSION.
     * 
     * <p>When auto-registration is enabled with FIND_OR_CREATE_VERSION, the serializer will automatically
     * register schemas that don't exist in the registry during serialization. If a schema with the same
     * content already exists in the registry, then the existing schema will be used. Otherwise a new version
     * will be created.</p>
     * 
     * <p>Note: This is the default behavior when auto-registration is enabled but no specific
     * IfArtifactExists option is set.</p>
     */
    public static void ConfigureAutoRegisterWithFindOrCreateVersionIfExists() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Enable auto-registration of schemas
        config.put(SchemaResolverProperties.AUTO_REGISTER_ARTIFACT, true);
        // Specifies the classpath resource path to the schema to be uploaded by auto-register.
        config.put(SchemaResolverProperties.SCHEMA_LOCATION, "json-schema/user.json");
        
        // Set behavior when artifact already exists to FIND_OR_CREATE_VERSION
        config.put(SchemaResolverProperties.AUTO_REGISTER_ARTIFACT_IF_EXISTS, 
                SchemaResolverProperties.IfArtifactExists.FIND_OR_CREATE_VERSION);

        // Create and configure JSON Schema serializer
        try (Serializer<User> serializer = new JsonSchemaSerializer<>()) {
            serializer.configure(config);

            // At this point, the JSON Schema serializer is configured and ready to use for serialization.
            // When serializing data with a schema that doesn't exist in the registry:
            // - The schema will be automatically registered in the registry
            // - If a schema with the same name already exists, it will use the existing schema
            //   if compatible, otherwise create a new version
        }
    }
}
