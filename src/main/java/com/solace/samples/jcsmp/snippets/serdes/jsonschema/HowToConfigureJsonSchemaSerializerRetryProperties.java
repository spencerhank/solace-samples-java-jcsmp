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
import com.solace.serdes.Serializer;
import com.solace.serdes.common.resolver.config.SchemaResolverProperties;
import com.solace.serdes.jsonschema.JsonSchemaSerializer;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides code snippets demonstrating how to configure schema registry retry
 * properties. This class includes scenarios for:
 * <ul>
 *   <li>Configuring the number of request attempts</li>
 *   <li>Configuring the backoff time between retry attempts</li>
 *   <li>Configuring a complete retry strategy</li>
 * </ul>
 */
public class HowToConfigureJsonSchemaSerializerRetryProperties {

    /**
     * Demonstrates how to configure the number of attempts to make when communicating
     * with the schema registry before giving up. When used with {@link SchemaResolverProperties#USE_CACHED_ON_ERROR},
     * this property specifies the number of attempts before falling back to the last cached value.
     * <p>Valid values are positive long values (1 - {@link Long#MAX_VALUE}).
     * <p>The default value is 3.
     */
    public static void SerializeWithRequestAttempts() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Example 1: Set number of request attempts using a Long value
        config.put(SchemaResolverProperties.REQUEST_ATTEMPTS, 5L);

        // Example 2: Set number of request attempts using a String value
        // config.put(SchemaResolverProperties.REQUEST_ATTEMPTS, "5");

        // Create and configure JSON Schema serializer
        try (Serializer<JsonNode> serializer = new JsonSchemaSerializer<>()) {
            serializer.configure(config);

            // At this point, the JSON Schema serializer is configured with the specified number of request attempts.
            // A higher value increases resilience during temporary registry availability issues.
            // Values less than or equal to 0 will result in an exception during configuration.
        }
    }

    /**
     * Demonstrates how to configure the backoff time in milliseconds between retry attempts
     * when communicating with the schema registry. This controls how long to wait before
     * trying again after a failed attempt.
     * <p>Valid values are non-negative long values (0 - {@link Long#MAX_VALUE}) or a Duration object.
     * <p>The default value is 500 milliseconds.
     */
    public static void SerializeWithRequestAttemptBackoff() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Example 1: Set backoff time using a Long value (milliseconds)
        config.put(SchemaResolverProperties.REQUEST_ATTEMPT_BACKOFF_MS, 1000L);

        // Example 2: Set backoff time using a String value (milliseconds)
        // config.put(SchemaResolverProperties.REQUEST_ATTEMPT_BACKOFF_MS, "1000");

        // Example 3: Set backoff time using a Duration object
        // config.put(SchemaResolverProperties.REQUEST_ATTEMPT_BACKOFF_MS, Duration.ofSeconds(1));

        // Example 4: No backoff between retry attempts
        // config.put(SchemaResolverProperties.REQUEST_ATTEMPT_BACKOFF_MS, 0L);

        // Create and configure JSON Schema serializer
        try (Serializer<JsonNode> serializer = new JsonSchemaSerializer<>()) {
            serializer.configure(config);

            // At this point, the JSON Schema serializer is configured with the specified backoff time.
            // Longer backoff times can help during temporary registry overload situations.
            // Shorter backoff times provide faster recovery when issues are brief.
            // A backoff of zero will cause immediate retries with no delay.
        }
    }

    /**
     * Demonstrates how to configure both request attempts and backoff time together
     * for a complete retry strategy.
     */
    public static void SerializeWithCompleteRetryStrategy() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Configure number of request attempts
        config.put(SchemaResolverProperties.REQUEST_ATTEMPTS, 5L);
        
        // Configure backoff time between attempts (2 seconds)
        config.put(SchemaResolverProperties.REQUEST_ATTEMPT_BACKOFF_MS, Duration.ofSeconds(2));
        
        // Optionally configure to use cached values on error
        config.put(SchemaResolverProperties.USE_CACHED_ON_ERROR, true);

        // Create and configure JSON Schema serializer
        try (Serializer<JsonNode> serializer = new JsonSchemaSerializer<>()) {
            serializer.configure(config);

            // At this point, the JSON Schema serializer is configured with a complete retry strategy:
            // - Up to 5 attempts will be made to contact the schema registry
            // - A 2-second backoff period will occur between attempts
            // - If all attempts fail, cached values will be used instead of throwing an exception
        }
    }
}
