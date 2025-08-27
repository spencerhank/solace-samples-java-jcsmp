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

import com.solace.serdes.Serializer;
import com.solace.serdes.avro.AvroProperties;
import com.solace.serdes.avro.AvroSerializer;
import com.solace.serdes.common.resolver.config.SchemaResolverProperties;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides code snippets demonstrating the serializer operation of Avro serializers
 * with Schema Registry resolution configurations. This class includes scenarios for:
 * <ul>
 *   <li>SerializeWithUserAvroSchema Serialize with sample avro User schema</li>
 *   <li>SerializeWithFindLatest Serialize with schema resolver 'find-latest' option</li>
 *   <li>SerializeWithAvroSchemaReferences Serialize an avro schema with references to another avro schema</li>
 *   <li>SerializeWithAutoRegisterAndAvroSchemaReferences Serialize an avro schema with references with auto-register</li>
 *   <li>SerializeWithExplicitSchemaVersion Serialize an avro User schema with an explicit version from the schema registry</li>
 *   <li>SerializeWithPrimitiveString Serialize with avro primitive string type</li>
 *   <li>SerializeWithPrimitiveInt Serialize with avro primitive int type</li>
 *   <li>SerializeWithPrimitiveLong Serialize with avro primitive long type</li>
 *   <li>SerializeWithPrimitiveFloat Serialize with avro primitive float type</li>
 *   <li>SerializeWithPrimitiveDouble Serialize with avro primitive double type</li>
 *   <li>SerializeWithPrimitiveBoolean Serialize with avro primitive boolean type</li>
 *   <li>SerializeWithPrimitiveNull Serialize with avro primitive null type</li>
 * </ul>
 */
public class HowToSerializeWithAvroSerializer {

    /**
     * Demonstrates how to serialize with the User avro schema.
     */
    public static void SerializeWithUserAvroSchema() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // create User Avro object
        GenericRecord user = initEmptyUserRecord();
        user.put("name", "John Doe");
        user.put("id", "-1");
        user.put("email", "support@solace.com");

        // Create and configure Avro serializer
        try (Serializer<GenericRecord> serializer = new AvroSerializer<>()) {
            serializer.configure(config);
            // create headers map for serialization
            Map<String, Object> headers = new HashMap<>();

            // At this point, the Avro serializer is configured and ready to use for serialization.
            byte[] userBytes = serializer.serialize("solace/samples/avro", user, headers);

            // at this point, userBytes and headers are ready to be applied to the messaging system of choice
            // userBytes are the user object serialized as bytes
            // headers were modified to hold the schema registry header fields to include:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long for schema identification
            // - {@link AvroHeaders#AVRO_ENCODING_TYPE} type String for avro encoded type
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

        // create User Avro object
        GenericRecord user = initEmptyUserRecord();
        user.put("name", "John Doe");
        user.put("id", "-1");
        user.put("email", "support@solace.com");

        // Create and configure Avro serializer
        try (Serializer<GenericRecord> serializer = new AvroSerializer<>()) {
            serializer.configure(config);
            // create headers map for serialization
            Map<String, Object> headers = new HashMap<>();

            // At this point, the Avro serializer is configured and ready to use for serialization.
            // The Serializer serialize will pull the latest version of the schema to resolve even if the resolver strategy
            // can load the schema.
            byte[] userBytes = serializer.serialize("solace/samples/avro", user, headers);

            // at this point, userBytes and headers are ready to be applied to the messaging system of choice
            // userBytes are the user object serialized as bytes
            // headers were modified to hold the schema registry header fields to include:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long for schema identification
            // - {@link AvroHeaders#AVRO_ENCODING_TYPE} type String for avro encoded type
        }
    }

    /**
     * Demonstrates how to serialize an avro schema with references to another avro schema.
     * Note: When working with schema references, the referenced schemas (e.g., User) must be
     * uploaded to the registry before the schemas that reference them (e.g., UserAccount).
     */
    public static void SerializeWithAvroSchemaReferences() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Set find latest artifact property
        config.put(SchemaResolverProperties.FIND_LATEST_ARTIFACT, true);

        // Create UserAccount Avro object with nested User object
        GenericRecord userAccount = initEmptyUserAccountRecord();
        userAccount.put("accountId", "-1");
        userAccount.put("isActive", true);

        // Create and set the nested User record
        GenericRecord user = initEmptyUserRecord();
        user.put("name", "John Doe");
        user.put("id", "-1");
        user.put("email", "support@solace.com");
        userAccount.put("user", user);

        // Create and configure Avro serializer
        try (Serializer<GenericRecord> serializer = new AvroSerializer<>()) {
            serializer.configure(config);
            // create headers map for serialization
            Map<String, Object> headers = new HashMap<>();

            // At this point, the Avro serializer is configured and ready to use for serialization.
            byte[] userAccountBytes = serializer.serialize("solace/samples/user-account/avro", userAccount, headers);

            // at this point, userAccountBytes and headers are ready to be applied to the messaging system of choice
            // userAccountBytes are the userAccount object serialized as bytes
            // headers were modified to hold the schema registry header fields to include:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long for schema identification
            // - {@link AvroHeaders#AVRO_ENCODING_TYPE} type String for avro encoded type
        }
    }

    /**
     * Demonstrates how to serialize an avro schema with references to another avro schema
     * with auto-registration enabled and dereferenced schema disabled for schema decomposition.
     * <p>
     * When dereferenced is set to false, the serializer treats the provided schema as one that 
     * needs to be decomposed into reference schemas. This enables a single Avro schema file to serve
     * as a reference for multiple related schemas within the registry.
     * <p>
     * Note: When working with schema references and auto-registration enabled, the serializer will
     * automatically register the referenced schema (User) first, followed by the schema that references it
     * (UserAccount). This ensures proper dependency resolution during the auto-registration process.
     */
    public static void SerializeWithAutoRegisterAndAvroSchemaReferences() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Enable auto-registration of schemas
        config.put(SchemaResolverProperties.AUTO_REGISTER_ARTIFACT, true);
        
        // Disable dereferenced schema to allow schema decomposition
        config.put(AvroProperties.DEREFERENCED_SCHEMA, false);

        // Create UserAccount Avro object with nested User object using the dereferenced schema
        GenericRecord userAccount = initEmptyUserAccountDereferencedRecord();
        userAccount.put("accountId", "ACC123456");
        userAccount.put("isActive", true);
        userAccount.put("user", initEmptyUserRecord());

        // Create and configure Avro serializer
        try (Serializer<GenericRecord> serializer = new AvroSerializer<>()) {
            serializer.configure(config);
            // create headers map for serialization
            Map<String, Object> headers = new HashMap<>();

            // At this point, the Avro serializer is configured and ready to use for serialization.
            // When serializing data with a schema that contains references to other schemas:
            // - The serializer will decompose the schema into reference schemas
            // - Each referenced schema will be registered separately in the registry
            byte[] userAccountBytes = serializer.serialize("solace/samples/avro", userAccount, headers);

            // at this point, userAccountBytes and headers are ready to be applied to the messaging system of choice
            // userAccountBytes are the userAccount object serialized as bytes
            // headers were modified to hold the schema registry header fields to include:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long for schema identification
            // - {@link AvroHeaders#AVRO_ENCODING_TYPE} type String for avro encoded type
        }
    }

    /**
     * Demonstrates how to serialize an avro User schema with an explicit version from the schema registry
     */
    public static void SerializeWithExplicitSchemaVersion() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // add property to resolve an explicit version of schema from the registry
        config.put(SchemaResolverProperties.EXPLICIT_ARTIFACT_VERSION, "1");

        // create User Avro object
        GenericRecord user = initEmptyUserRecord();
        user.put("name", "John Doe");
        user.put("id", "-1");
        user.put("email", "support@solace.com");

        // Create and configure Avro serializer
        try (Serializer<GenericRecord> serializer = new AvroSerializer<>()) {
            serializer.configure(config);
            // create headers map for serialization
            Map<String, Object> headers = new HashMap<>();

            // At this point, the Avro serializer is configured and ready to use for serialization.
            // The call to serialize will pull the explicit version of the schema to resolve even if the resolver strategy
            // can load the schema.
            byte[] userBytes = serializer.serialize("solace/samples/avro", user, headers);

            // at this point, userBytes and headers are ready to be applied to the messaging system of choice
            // userBytes are the user object serialized as bytes
            // headers were modified to hold the schema registry header fields to include:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long for schema identification
            // - {@link AvroHeaders#AVRO_ENCODING_TYPE} type String for avro encoded type
        }
    }

    /**
     * Demonstrates how to serialize with the avro primitive type String.
     */
    public static void SerializeWithPrimitiveString() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // create payload string
        String payload = "helloworld";

        // Create and configure Avro serializer
        try (Serializer<String> serializer = new AvroSerializer<>()) {
            serializer.configure(config);
            Map<String, Object> headers = new HashMap<>();

            // At this point, the Avro serializer is configured and ready to use for serialization.
            // Serializer will generate a primitive schema equivalent to Schema.create(Schema.TYPE.STRING).toString()
            // For example:
            // {"type": "string"}
            byte[] payloadBytes = serializer.serialize("solace/samples/avro", payload, headers);

            // at this point, payloadBytes and headers are ready to be applied to the messaging system of choice
            // payloadBytes are the string serialized as bytes
            // headers were modified to hold the schema registry header fields to include:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long for schema identification
            // - {@link AvroHeaders#AVRO_ENCODING_TYPE} type String for avro encoded type
        }
    }

    /**
     * Demonstrates how to serialize with the avro primitive type Int.
     */
    public static void SerializeWithPrimitiveInt() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // create payload int
        int payload = 42;

        // Create and configure Avro serializer
        try (Serializer<Integer> serializer = new AvroSerializer<>()) {
            serializer.configure(config);
            Map<String, Object> headers = new HashMap<>();

            // At this point, the Avro serializer is configured and ready to use for serialization.
            // Serializer will generate a primitive schema equivalent to Schema.create(Schema.TYPE.INT).toString()
            // For example:
            // {"type": "int"}
            byte[] payloadBytes = serializer.serialize("solace/samples/avro", payload, headers);

            // at this point, payloadBytes and headers are ready to be applied to the messaging system of choice
            // payloadBytes are the int object serialized as bytes
            // headers were modified to hold the schema registry header fields to include:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long for schema identification
            // - {@link AvroHeaders#AVRO_ENCODING_TYPE} type String for avro encoded type
        }
    }

    /**
     * Demonstrates how to serialize with the avro primitive type Long.
     */
    public static void SerializeWithPrimitiveLong() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // create payload long
        long payload = 42L;

        // Create and configure Avro serializer
        try (Serializer<Long> serializer = new AvroSerializer<>()) {
            serializer.configure(config);
            Map<String, Object> headers = new HashMap<>();

            // At this point, the Avro serializer is configured and ready to use for serialization.
            // Serializer will generate a primitive schema equivalent to Schema.create(Schema.TYPE.LONG).toString()
            // For example:
            // {"type": "long"}
            byte[] payloadBytes = serializer.serialize("solace/samples/avro", payload, headers);

            // at this point, payloadBytes and headers are ready to be applied to the messaging system of choice
            // payloadBytes are the long object serialized as bytes
            // headers were modified to hold the schema registry header fields to include:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long for schema identification
            // - {@link AvroHeaders#AVRO_ENCODING_TYPE} type String for avro encoded type
        }
    }

    /**
     * Demonstrates how to serialize with the avro primitive type Float.
     */
    public static void SerializeWithPrimitiveFloat() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // create payload float
        float payload = 3.14159f;

        // Create and configure Avro serializer
        try (Serializer<Float> serializer = new AvroSerializer<>()) {
            serializer.configure(config);
            Map<String, Object> headers = new HashMap<>();

            // At this point, the Avro serializer is configured and ready to use for serialization.
            // Serializer will generate a primitive schema equivalent to Schema.create(Schema.TYPE.FLOAT).toString()
            // For example:
            // {"type": "float"}
            byte[] payloadBytes = serializer.serialize("solace/samples/avro", payload, headers);

            // at this point, payloadBytes and headers are ready to be applied to the messaging system of choice
            // payloadBytes are the float object serialized as bytes
            // headers were modified to hold the schema registry header fields to include:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long for schema identification
            // - {@link AvroHeaders#AVRO_ENCODING_TYPE} type String for avro encoded type
        }
    }

    /**
     * Demonstrates how to serialize with the avro primitive type Double.
     */
    public static void SerializeWithPrimitiveDouble() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // create payload float
        double payload = 2.172D;

        // Create and configure Avro serializer
        try (Serializer<Double> serializer = new AvroSerializer<>()) {
            serializer.configure(config);
            Map<String, Object> headers = new HashMap<>();

            // At this point, the Avro serializer is configured and ready to use for serialization.
            // Serializer will generate a primitive schema equivalent to Schema.create(Schema.TYPE.DOUBLE).toString()
            // For example:
            // {"type": "double"}
            byte[] payloadBytes = serializer.serialize("solace/samples/avro", payload, headers);

            // at this point, payloadBytes and headers are ready to be applied to the messaging system of choice
            // payloadBytes are the double object serialized as bytes
            // headers were modified to hold the schema registry header fields to include:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long for schema identification
            // - {@link AvroHeaders#AVRO_ENCODING_TYPE} type String for avro encoded type
        }
    }

    /**
     * Demonstrates how to serialize with the avro primitive type Boolean.
     */
    public static void SerializeWithPrimitiveBoolean() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // create payload boolean
        boolean payload = true;

        // Create and configure Avro serializer
        try (Serializer<Boolean> serializer = new AvroSerializer<>()) {
            serializer.configure(config);
            Map<String, Object> headers = new HashMap<>();

            // At this point, the Avro serializer is configured and ready to use for serialization.
            // Serializer will generate a primitive schema equivalent to Schema.create(Schema.TYPE.BOOLEAN).toString()
            // For example:
            // {"type": "boolean"}
            byte[] payloadBytes = serializer.serialize("solace/samples/avro", payload, headers);

            // at this point, payloadBytes and headers are ready to be applied to the messaging system of choice
            // payloadBytes are the boolean object serialized as bytes
            // headers were modified to hold the schema registry header fields to include:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long for schema identification
            // - {@link AvroHeaders#AVRO_ENCODING_TYPE} type String for avro encoded type
        }
    }

    /**
     * Demonstrates how to serialize with the avro primitive type Null.
     */
    public static void SerializeWithPrimitiveNull() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // create payload null
        Object payload = null;

        // Create and configure Avro serializer
        try (Serializer<Object> serializer = new AvroSerializer<>()) {
            serializer.configure(config);
            Map<String, Object> headers = new HashMap<>();

            // At this point, the Avro serializer is configured and ready to use for serialization.
            // Serializer will generate a primitive schema equivalent to Schema.create(Schema.TYPE.NULL).toString()
            // For example:
            // {"type": "null"}
            byte[] payloadBytes = serializer.serialize("solace/samples/avro", payload, headers);

            // at this point, payloadBytes and headers are ready to be applied to the messaging system of choice
            // payloadBytes are the null object serialized as bytes
            // headers were modified to hold the schema registry header fields to include:
            // - {@link SerdeHeaders#SCHEMA_ID} type Long for schema identification
            // - {@link AvroHeaders#AVRO_ENCODING_TYPE} type String for avro encoded type
        }
    }

    /**
     * Initializes an empty Avro GenericRecord based on the "user.avsc" schema.
     *
     * @return An empty GenericRecord
     * @throws IOException If there's an error reading the schema file
     */
    private static GenericRecord initEmptyUserRecord() throws IOException {
        try (InputStream rawSchema = HowToSerializeWithAvroSerializer.class.getResourceAsStream("/avro-schema/user.avsc")) {
            Schema schema = new SchemaParser().parse(rawSchema).mainSchema();
            return new GenericData.Record(schema);
        }
    }

    /**
     * Initializes an empty Avro GenericRecord based on the "user-account.avsc" schema.
     * Properly handles the User schema reference in the UserAccount schema.
     *
     * @return An empty GenericRecord for UserAccount
     * @throws IOException If there's an error reading the schema files
     */
    private static GenericRecord initEmptyUserAccountRecord() throws IOException {
        // First, parse the User schema which is referenced by UserAccount
        SchemaParser parser = new SchemaParser();
        try (InputStream userSchema = HowToSerializeWithAvroSerializer.class.getResourceAsStream("/avro-schema/user.avsc")) {
            parser.parse(userSchema);
        }
        // Now parse the UserAccount schema which contains references to User
        try (InputStream accountSchema = HowToSerializeWithAvroSerializer.class.getResourceAsStream("/avro-schema/user-account.avsc")) {
            Schema schema = parser.parse(accountSchema).mainSchema();
            return new GenericData.Record(schema);
        }
    }
    
    /**
     * Initializes an empty Avro GenericRecord based on the "user-account-dereferenced.avsc" schema.
     * This schema contains the User record embedded directly within the UserAccount schema.
     *
     * @return An empty GenericRecord for UserAccount with an embedded User record
     * @throws IOException If there's an error reading the schema file
     */
    private static GenericRecord initEmptyUserAccountDereferencedRecord() throws IOException {
        // Parse the dereferenced UserAccount schema which contains the User schema embedded within it
        try (InputStream accountSchema = HowToSerializeWithAvroSerializer.class.getResourceAsStream("/avro-schema/user-account-dereferenced.avsc")) {
            SchemaParser parser = new SchemaParser();
            Schema schema = parser.parse(accountSchema).mainSchema();
            return new GenericData.Record(schema);
        }
    }
}
