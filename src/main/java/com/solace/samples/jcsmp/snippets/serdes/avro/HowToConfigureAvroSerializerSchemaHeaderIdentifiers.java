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
import com.solace.serdes.avro.AvroSerializer;
import com.solace.serdes.common.SchemaHeaderId;
import com.solace.serdes.common.SerdeProperties;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides code snippets demonstrating how to configure Avro serializers with schema header identifiers.
 * This class includes scenarios for:
 * <ul>
 *   <li>Configuring with the default identifier</li>
 *   <li>Configuring with a single identifier from a string</li>
 *   <li>Configuring with a single identifier from an enum</li>
 *   <li>Configuring with multiple identifiers</li>
 * </ul>
 */
public class HowToConfigureAvroSerializerSchemaHeaderIdentifiers {

    /**
     * Demonstrates how to configure an Avro serializer to use the default schema header identifier {@link SchemaHeaderId#SCHEMA_ID SCHEMA_ID}.
     */
    public static void configureWithDefaultIdentifier() throws IOException {
        Map<String, Object> config = new HashMap<>();
        // SCHEMA_ID is the default, so this configuration is redundant, but shown for clarity.
        config.put(SerdeProperties.SCHEMA_HEADER_IDENTIFIERS, SchemaHeaderId.SCHEMA_ID);

        try (Serializer<GenericRecord> serializer = new AvroSerializer<>()) {
            serializer.configure(config);
            // On Serializer.serialize(String, Object, Map) headers will be populated with SerdeHeaders.SCHEMA_ID.
        }
    }

    /**
     * Demonstrates how to configure an Avro serializer to use a single schema header identifier from a string.
     * This example uses {@code "SCHEMA_ID_STRING"}.
     */
    public static void configureWithSingleIdentifierFromString() throws IOException {
        Map<String, Object> config = new HashMap<>();
        // Using a String for a single identifier.
        config.put(SerdeProperties.SCHEMA_HEADER_IDENTIFIERS, "SCHEMA_ID_STRING");

        try (Serializer<GenericRecord> serializer = new AvroSerializer<>()) {
            serializer.configure(config);
            // On Serializer.serialize(String, Object, Map) headers will be populated with SerdeHeaders.SCHEMA_ID_STRING.
        }
    }

    /**
     * Demonstrates how to configure an Avro serializer to use a single schema header identifier from an enum.
     * This example uses {@link SchemaHeaderId#SCHEMA_ID_STRING SCHEMA_ID_STRING}.
     */
    public static void configureWithSingleIdentifierFromEnum() throws IOException {
        Map<String, Object> config = new HashMap<>();
        // Using an Enum constant for a single identifier.
        config.put(SerdeProperties.SCHEMA_HEADER_IDENTIFIERS, SchemaHeaderId.SCHEMA_ID_STRING);

        try (Serializer<GenericRecord> serializer = new AvroSerializer<>()) {
            serializer.configure(config);
            // On Serializer.serialize(String, Object, Map) headers will be populated with SerdeHeaders.SCHEMA_ID_STRING.
        }
    }

    /**
     * Demonstrates how to configure an Avro serializer to use multiple schema header identifiers.
     * This example uses both {@link SchemaHeaderId#SCHEMA_ID_STRING SCHEMA_ID_STRING} & {@link SchemaHeaderId#SCHEMA_ID SCHEMA_ID}.
     */
    public static void configureWithMultipleIdentifiers() throws IOException {
        Map<String, Object> config = new HashMap<>();
        // Using an EnumSet for multiple identifiers.
        config.put(SerdeProperties.SCHEMA_HEADER_IDENTIFIERS,
                EnumSet.of(SchemaHeaderId.SCHEMA_ID, SchemaHeaderId.SCHEMA_ID_STRING));

        try (Serializer<GenericRecord> serializer = new AvroSerializer<>()) {
            serializer.configure(config);
            // On Serializer.serialize(String, Object, Map) headers will be populated with SerdeHeaders.SCHEMA_ID
            // and SerdeHeaders.SCHEMA_ID_STRING.
        }
    }
}
