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

import com.solace.serdes.avro.AvroDeserializer;
import com.solace.serdes.common.resolver.config.SchemaResolverProperties;
import org.apache.avro.generic.GenericRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * Provides code snippets demonstrating how to configure Avro deserializers
 * with different cache properties. This class includes scenarios for:
 * <ul>
 *   <li>Configuring the cache TTL (time-to-live) property</li>
 *   <li>Configuring the use-cached-on-error property</li>
 * </ul>
 */
public class HowToConfigureAvroDeserializerCacheProperties {

    /**
     * Demonstrates how to configure the cache TTL (time-to-live) property in milliseconds for an Avro deserializer.
     * The cache TTL determines how long schema artifacts remain valid in the cache before
     * they need to be fetched again from the registry on the next relevant lookup.
     * <p> The default value is 30000 ms (30 seconds).
     */
    public static void DeserializeWithCacheTTL() {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Example 1: Set cache TTL using a Long value
        config.put(SchemaResolverProperties.CACHE_TTL_MS, 5000L);

        // Example 2: Set cache TTL using a String value
        // config.put(SchemaResolverProperties.CACHE_TTL_MS, "5000");

        // Example 3: Set cache TTL using a Duration object
        // config.put(SchemaResolverProperties.CACHE_TTL_MS, Duration.ofSeconds(5));

        // Example 4: Disable caching completely.
        // config.put(SchemaResolverProperties.CACHE_TTL_MS, 0L);

        // Create and configure Avro deserializer
        try (AvroDeserializer<GenericRecord> deserializer = new AvroDeserializer<>()) {
            deserializer.configure(config);

            // At this point, the Avro deserializer is configured with the specified cache TTL.
            // A longer TTL improves performance by reducing registry calls but may use outdated schemas.
            // A shorter TTL ensures more up-to-date schemas but increases load on the registry.
            // A TTL of zero disables caching, so schemas will be fetched from the registry on every request.
        }
    }

    /**
     * Demonstrates how to configure the use-cached-on-error property for an Avro deserializer.
     * This controls whether to use cached schemas when schema registry lookup errors occur.
     * <p> The default value is false.
     */
    public static void DeserializeWithUseCachedOnError() {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Example 1: Use cached schemas when registry lookups fail (resilient mode)
        config.put(SchemaResolverProperties.USE_CACHED_ON_ERROR, true);

        // Example 2: Throw exceptions when registry lookups fail (strict mode, default value)
        // config.put(SchemaResolverProperties.USE_CACHED_ON_ERROR, false);

        // Create and configure Avro deserializer
        try (AvroDeserializer<GenericRecord> deserializer = new AvroDeserializer<>()) {
            deserializer.configure(config);

            // At this point, the Avro deserializer is configured with the specified use-cached-on-error property.
            // When enabled, schema resolution will use cached schemas instead of throwing exceptions
            // after retry attempts are exhausted, improving resilience during registry outages.
            // When disabled, exceptions will be thrown when registry lookup errors occur.
        }
    }
}
