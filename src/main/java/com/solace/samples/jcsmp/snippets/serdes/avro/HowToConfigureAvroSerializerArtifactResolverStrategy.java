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
import com.solace.serdes.avro.strategy.RecordIdStrategy;
import com.solace.serdes.common.SerdeRecord;
import com.solace.serdes.common.resolver.ParsedSchema;
import com.solace.serdes.common.resolver.config.SchemaResolverProperties;
import com.solace.serdes.common.resolver.data.Record;
import com.solace.serdes.common.resolver.strategy.ArtifactReference;
import com.solace.serdes.common.resolver.strategy.ArtifactReferenceBuilder;
import com.solace.serdes.common.resolver.strategy.ArtifactReferenceResolverStrategy;
import com.solace.serdes.common.resolver.strategy.DestinationIdStrategy;
import com.solace.serdes.common.resolver.strategy.SolaceTopicArtifactMapping;
import com.solace.serdes.common.resolver.strategy.SolaceTopicIdStrategy;
import com.solace.serdes.common.resolver.strategy.SolaceTopicProfile;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides code snippets demonstrating how to configure Avro serializers
 * with different ArtifactReferenceResolverStrategy implementations.
 * This class includes scenarios for:
 * <ul>
 *   <li>Configuring a pre-defined strategy (e.g. RecordIdStrategy, DestinationIdStrategy, SolaceTopicIdStrategy) using a Class object</li>
 *   <li>Configuring a SolaceTopicProfile for SolaceTopicIdStrategy</li>
 *   <li>Advanced configuration of a SolaceTopicProfile and SolaceTopicArtifactMapping</li>
 *   <li>Configuring a custom strategy using its fully qualified class name</li>
 *   <li>Configuring a custom strategy using a Class object</li>
 * </ul>
 */
public class HowToConfigureAvroSerializerArtifactResolverStrategy {

    /**
     * Demonstrates how to configure an Avro serializer using {@link RecordIdStrategy} Class object.
     * The {@link RecordIdStrategy} extracts the schema from the record's payload, then uses the schema's
     * name as the artifactId and the schema's namespace as the groupId to build
     * the {@link ArtifactReference}.
     */
    public static void SerializeWithRecordIdStrategy() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Set the ArtifactReferenceResolverStrategy using RecordIdStrategy Class object
        // NOTE: The ArtifactReferenceResolverStrategy must have parameterless constructor
        config.put(SchemaResolverProperties.ARTIFACT_RESOLVER_STRATEGY, RecordIdStrategy.class);

        // Create and configure Avro serializer
        try (Serializer<GenericRecord> serializer = new AvroSerializer<>()) {
            serializer.configure(config);

            // At this point, the Avro serializer is configured and ready to use for serialization.
            // The RecordIdStrategy will be used to determine the specific ArtifactReference for a given data record,
            // enabling the system to locate the correct schema in the registry.
        }
    }

    /**
     * Demonstrates how to configure an Avro serializer using {@link DestinationIdStrategy} Class object.
     * The {@link DestinationIdStrategy} uses the destination name from the record's metadata as the artifactId
     * and default as the groupId to build the {@link ArtifactReference}.
     */
    public static void SerializeWithDestinationIdStrategy() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Set the ArtifactReferenceResolverStrategy using DestinationIdStrategy Class object
        // NOTE: The ArtifactReferenceResolverStrategy must have parameterless constructor
        config.put(SchemaResolverProperties.ARTIFACT_RESOLVER_STRATEGY, DestinationIdStrategy.class);

        // Create and configure Avro serializer
        try (Serializer<GenericRecord> serializer = new AvroSerializer<>()) {
            serializer.configure(config);

            // At this point, the Avro serializer is configured and ready to use for serialization.
            // The DestinationIdStrategy will be used to determine the specific ArtifactReference for a given data record,
            // enabling the system to locate the correct schema in the registry.
        }
    }

    /**
     * Demonstrates how to configure an Avro serializer using {@link SolaceTopicIdStrategy} Class object.
     * The {@link SolaceTopicIdStrategy} maps the destination name to the {@link ArtifactReference} setting the
     * artifact id as the destination string.
     */
    public static void SerializeWithSolaceTopicIdStrategyWithoutProfile() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Set the ArtifactReferenceResolverStrategy using SolaceTopicIdStrategy Class object
        // NOTE: The ArtifactReferenceResolverStrategy must have parameterless constructor
        config.put(SchemaResolverProperties.ARTIFACT_RESOLVER_STRATEGY, SolaceTopicIdStrategy.class);

        // Create and configure Avro serializer
        try (Serializer<GenericRecord> serializer = new AvroSerializer<>()) {
            serializer.configure(config);

            // At this point, the Avro serializer is configured and ready to use for serialization.
            // The SolaceTopicIdStrategy will be used to determine the specific ArtifactReference for a given data record,
            // enabling the system to locate the correct schema in the registry.
        }
    }

    /**
     * Demonstrates how to create a {@link SolaceTopicArtifactMapping} with only a topic expression to be used to create
     * a {@link SolaceTopicProfile}.
     *
     * @param profile The SolaceTopicProfile to add mappings to.
     */
    public static void createSolaceTopicArtifactMappingWithTopicExpressionOnly(SolaceTopicProfile profile) {
        // create mapping with literal expression
        SolaceTopicArtifactMapping mapping1 = SolaceTopicArtifactMapping.create("solace/samples/avro");
        // this generates an ArtifactReference with:
        //      group id: 'default', accessible with mapping1.getArtifactReference().getGroupId()
        //      artifact id: 'solace/samples/avro' accessible with mapping1.getArtifactReference().getArtifactId()

        // create mapping with single level wildcard '*'
        SolaceTopicArtifactMapping mapping2 = SolaceTopicArtifactMapping.create("solace/*/sample/avro");
        // this generates an ArtifactReference with:
        //      group id: 'default', accessible with mapping2.getArtifactReference().getGroupId()
        //      artifact id: 'solace/*/sample/avro' accessible with mapping2.getArtifactReference().getArtifactId()

        // create mapping with multi level wildcard '>'
        SolaceTopicArtifactMapping mapping3 = SolaceTopicArtifactMapping.create("solace/>");
        // this generates an ArtifactReference with:
        //      group id: 'default', accessible with mapping3.getArtifactReference().getGroupId()
        //      artifact id: 'solace/>' accessible with mapping3.getArtifactReference().getArtifactId()

        // See, https://docs.solace.com/Messaging/Wildcard-Charaters-Topic-Subs.htm for more details on wildcard rules.

        // At this point the profile can add the mappings using: profile.add(...)
        profile.add(mapping1);
        profile.add(mapping2);
        profile.add(mapping3);
    }

    /**
     * Demonstrates how to create a {@link SolaceTopicArtifactMapping} with a topic expression and artifact id to be
     * used to create a {@link SolaceTopicProfile}.
     *
     * @param profile The SolaceTopicProfile to add mappings to.
     */
    public static void createSolaceTopicArtifactMappingWithTopicExpressionAndArtifactId(SolaceTopicProfile profile) {
        // create mapping with literal expression and artifact id
        SolaceTopicArtifactMapping mapping1 = SolaceTopicArtifactMapping.create("solace/samples/avro", "User");
        // this generates an ArtifactReference with:
        //      group id: 'default', accessible with mapping1.getArtifactReference().getGroupId()
        //      artifact id: 'User' accessible with mapping1.getArtifactReference().getArtifactId()

        // create mapping with single level wildcard '*' and artifact id
        SolaceTopicArtifactMapping mapping2 = SolaceTopicArtifactMapping.create("solace/*/sample/avro", "NewUser");
        // this generates an ArtifactReference with:
        //      group id: 'default', accessible with mapping2.getArtifactReference().getGroupId()
        //      artifact id: 'NewUser' accessible with mapping2.getArtifactReference().getArtifactId()

        // create mapping with multi level wildcard '>' and artifact id
        SolaceTopicArtifactMapping mapping3 = SolaceTopicArtifactMapping.create("solace/>", "OldUser");
        // this generates an ArtifactReference with:
        //      group id: 'default', accessible with mapping3.getArtifactReference().getGroupId()
        //      artifact id: 'OldUser' accessible with mapping3.getArtifactReference().getArtifactId()

        // See, https://docs.solace.com/Messaging/Wildcard-Charaters-Topic-Subs.htm for more details on wildcard rules.

        // At this point the profile can add the mappings using: profile.add(...)
        profile.add(mapping1);
        profile.add(mapping2);
        profile.add(mapping3);
    }

    /**
     * Demonstrates how to create a {@link SolaceTopicArtifactMapping} with a topic expression and
     * {@link ArtifactReference} for advanced mapping scenarios to be used to create a {@link SolaceTopicProfile}.
     *
     * @param profile The SolaceTopicProfile to add mappings to.
     */
    public static void createSolaceTopicArtifactMappingWithTopicExpressionAndArtifactReference(SolaceTopicProfile profile) {
        // create Artifact reference builder
        ArtifactReferenceBuilder builder = new ArtifactReferenceBuilder();
        // create artifact reference for mapping
        ArtifactReference reference = builder
                .groupId("com.solace.samples.serdes.avro.schema")
                .artifactId("User")
                .build();
        // create mapping with literal expression and artifact reference
        SolaceTopicArtifactMapping mapping1 = SolaceTopicArtifactMapping.create("solace/samples/avro", reference);
        // this generates an ArtifactReference with:
        //      group id: 'com.solace.samples.serdes.avro.schema', accessible with mapping1.getArtifactReference().getGroupId()
        //      artifact id: 'User' accessible with mapping1.getArtifactReference().getArtifactId()

        // create new artifact reference for mapping
        reference = builder
                .groupId("com.solace.samples.serdes.avro.schema")
                .artifactId("NewUser")
                .build();
        // create mapping with single level wildcard '*' and artifact reference
        SolaceTopicArtifactMapping mapping2 = SolaceTopicArtifactMapping.create("solace/*/sample/avro", reference);
        // this generates an ArtifactReference with:
        //      group id: 'com.solace.samples.serdes.avro.schema', accessible with mapping2.getArtifactReference().getGroupId()
        //      artifact id: 'NewUser' accessible with mapping2.getArtifactReference().getArtifactId()

        // create new artifact reference for mapping with an explicit version
        reference = builder
                .groupId("com.solace.samples.serdes.avro.schema")
                .artifactId("User")
                .version("0.0.1")
                .build();
        // create mapping with multi level wildcard '>' and artifact reference
        SolaceTopicArtifactMapping mapping3 = SolaceTopicArtifactMapping.create("solace/>", reference);
        // this generates an ArtifactReference with:
        //      group id: 'default', accessible with mapping3.getArtifactReference().getGroupId()
        //      artifact id: 'User', accessible with mapping3.getArtifactReference().getArtifactId()
        //      version: '0.0.1', accessible with mapping3.getArtifactReference().getVersion()

        // See, https://docs.solace.com/Messaging/Wildcard-Charaters-Topic-Subs.htm for more details on wildcard rules.

        // At this point the profile can add the mappings using: profile.add(...)
        profile.add(mapping1);
        profile.add(mapping2);
        profile.add(mapping3);
    }

    /**
     * Demonstrates how to modify a {@link SolaceTopicProfile} with {@link SolaceTopicProfile#clear()}
     *
     * @param profile the {@link SolaceTopicProfile} to modify with one or more mapping already
     */
    public static void modifySolaceTopicProfileWithClear(SolaceTopicProfile profile){
        profile.clear(); // clears the mapping list returned by profile.getMappings()
        // profile.getMappings().size() should now be == 0
    }

    /**
     * Demonstrates how to modify a {@link SolaceTopicProfile} with {@link SolaceTopicProfile#add(SolaceTopicArtifactMapping)}
     *
     * @param profile the {@link SolaceTopicProfile} to modify with 0 or more mappings already
     * @param mappingToAdd the {@link SolaceTopicArtifactMapping} to add to the profile
     */
    public static void modifySolaceTopicProfileWithAdd(SolaceTopicProfile profile, SolaceTopicArtifactMapping mappingToAdd){
        profile.add(mappingToAdd); // appends mapping to the end of the profile.getMappings() list
        // now profile.getMappings().get(profile.getMappings().size() -1) should equal mappingToAdd
    }

    /**
     * Demonstrates how to modify a {@link SolaceTopicProfile} with {@link SolaceTopicProfile#remove(int)}
     *
     * @param profile the {@link SolaceTopicProfile} to modify with 1 or more mappings already
     * @param mappingIndexToRemove the index in the profile for removal
     */
    public static void modifySolaceTopicProfileWithRemoveByIndex(SolaceTopicProfile profile, int mappingIndexToRemove){
        // removes mapping match the list index from profile.getMappings() list
        SolaceTopicArtifactMapping removedMapping = profile.remove(mappingIndexToRemove);
        // where the removedMapping is the mapping object removed or raises IllegalArgumentException for invalid index
    }

    /**
     * Demonstrates how to modify a {@link SolaceTopicProfile} with {@link SolaceTopicProfile#remove(String)}
     *
     * @param profile the {@link SolaceTopicProfile} to modify with 1 or more mappings already
     * @param topicExpression the string expression for the mapping in the profile for removal
     */
    public static void modifySolaceTopicProfileWithRemoveTopicExpression(SolaceTopicProfile profile, String topicExpression){
        // removes the first mapping to match the topic expression in the list from profile.getMappings()
        SolaceTopicArtifactMapping removedMapping = profile.remove(topicExpression);
        // where the removedMapping is the mapping object removed or returns null is there are no matching mapping
        // with given topic expression
    }

    /**
     * Demonstrates how to modify a {@link SolaceTopicProfile} with {@link SolaceTopicProfile#remove(SolaceTopicArtifactMapping)}
     *
     * @param profile the {@link SolaceTopicProfile} to modify with 1 or more mappings already
     * @param mappingToRemove the {@link SolaceTopicArtifactMapping} in the profile for removal
     */
    public static void modifySolaceTopicProfileWithRemoveMapping(SolaceTopicProfile profile, SolaceTopicArtifactMapping mappingToRemove){
        // removes the first mapping to equal the mappingToRemove object in the list from profile.getMappings()
        SolaceTopicArtifactMapping removedMapping = profile.remove(mappingToRemove);
        // where the removedMapping is the mapping object removed or returns null is there are no matching mapping
        // with given mappingToRemove object
    }

    /**
     * Demonstrates how to configure an Avro serializer using {@link SolaceTopicIdStrategy} Class object.
     * The {@link SolaceTopicIdStrategy} maps the destination name to the {@link ArtifactReference} setting the
     * artifact id as the destination string.
     * <br/>
     * For more profile configuration options see,
     * <ul>
     *     <li>{@link HowToConfigureAvroSerializerArtifactResolverStrategy#createSolaceTopicArtifactMappingWithTopicExpressionOnly(SolaceTopicProfile)}</li>
     *     <li>{@link HowToConfigureAvroSerializerArtifactResolverStrategy#createSolaceTopicArtifactMappingWithTopicExpressionAndArtifactId(SolaceTopicProfile)}</li>
     *     <li>{@link HowToConfigureAvroSerializerArtifactResolverStrategy#createSolaceTopicArtifactMappingWithTopicExpressionAndArtifactReference(SolaceTopicProfile)}</li>
     *     <li>{@link HowToConfigureAvroSerializerArtifactResolverStrategy#modifySolaceTopicProfileWithAdd(SolaceTopicProfile, SolaceTopicArtifactMapping)}</li>
     * </ul>
     */
    public static void SerializeWithSolaceTopicIdStrategyWithProfile() throws IOException {
        // create Topic profile for mapping of solace smart topics
        SolaceTopicProfile profile = SolaceTopicProfile.create(); // creates empty profile

        // entries for mappings are ordered and the matching mapping is selected by the SolaceTopicIdStrategy

        // create Mapping to add to the profile
        // mappings required a topic expression to match on given destination name from serialize
        // topic expressions can be literal expressions without wildcards

        // create mapping using only topic expression which generates an artifact reference
        // with artifact id with the topic expression string
        profile.add(SolaceTopicArtifactMapping.create("solace/samples/avro"));
        // as a literal mapping this topic expression will only match the destination name 'solace/samples/avro'

        // topic expressions can be literal expressions with wildcards like '*' and '>'
        // See, https://docs.solace.com/Messaging/Wildcard-Charaters-Topic-Subs.htm for wildcard rules.
        // create another mapping with wildcard '>' which maps to custom artifact id 'User'
        profile.add(SolaceTopicArtifactMapping.create("solace/>", "User"));
        // as a wildcard mapping this topic expression will match many the destination names,
        // for example 'solace/other', 'solace/a', 'solace/a/b', etc

        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Set the ArtifactReferenceResolverStrategy using SolaceTopicIdStrategy Class object
        // NOTE: The ArtifactReferenceResolverStrategy must have parameterless constructor
        config.put(SchemaResolverProperties.ARTIFACT_RESOLVER_STRATEGY, SolaceTopicIdStrategy.class);
        // Set the topic Profile using the configure profile
        // the profile must contain at least one mapping or the serializer can not be configured with it
        config.put(SchemaResolverProperties.STRATEGY_TOPIC_PROFILE, profile);

        // Create and configure Avro serializer
        try (Serializer<GenericRecord> serializer = new AvroSerializer<>()) {
            serializer.configure(config);

            // At this point, the Avro serializer is configured and ready to use for serialization.
            // The SolaceTopicIdStrategy will be used to determine the specific ArtifactReference for a given data record,
            // enabling the system to locate the correct schema in the registry.

            // for an example serialize call: serializer.serialize(destinationName, record, headers);
            // Given the following destination names the following expected coordinate will be selected:
            // - destination name 'solace/samples/avro' matches topic expression: 'solace/samples/avro'
            //   and selects, group id: 'default', artifact id: 'solace/samples/avro'
            // - destination name 'solace/other/sample' matches topic expression: 'solace/>'
            //   and selects, group id: 'default', artifact id: 'User'
            // - destination name 'pubsub/samples' has no matching topic expression,
            //   and can not select an artifact reference throwing a SerializationException

            // Note: order matters for topic profiles as 'solace/>' can also match 'solace/samples/avro' however as
            //       there is a match before the wildcard mapping selecting that reference instead.
        }
    }


    /**
     * Demonstrates how to configure an Avro serializer with a custom ArtifactReferenceResolverStrategy
     * using its fully qualified class name.
     */
    public static void SerializeWithCustomArtifactResolverStrategyUsingClassName() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Set the custom ArtifactReferenceResolverStrategy using its fully qualified class name
        // Instead of using CustomArtifactReferenceResolverStrategy.class.getName(), you can also pass the String directly.
        // For instance: "com.solace.sample.snippets.avro.HowToConfigureAvroSerializerArtifactResolverStrategy$CustomArtifactReferenceResolverStrategy"
        // NOTE: The ArtifactReferenceResolverStrategy must have parameterless constructor
        config.put(SchemaResolverProperties.ARTIFACT_RESOLVER_STRATEGY, CustomArtifactReferenceResolverStrategy.class.getName());

        // Create and configure Avro serializer
        try (Serializer<GenericRecord> serializer = new AvroSerializer<>()) {
            serializer.configure(config);

            // At this point, the Avro serializer is configured and ready to use for serialization.
            // The ArtifactReferenceResolverStrategy will be used to determine the specific
            // ArtifactReference for a given data record, enabling the system to locate the correct schema in the registry.
        }
    }

    /**
     * Demonstrates how to configure an Avro serializer with a custom ArtifactReferenceResolverStrategy
     * using a Class object.
     */
    public static void SerializeWithCustomArtifactResolverStrategyUsingObject() throws IOException {
        // Create configuration map
        Map<String, Object> config = new HashMap<>();

        // Set required Schema Registry connection properties

        // Set the custom ArtifactReferenceResolverStrategy using a Class object
        // NOTE: The ArtifactReferenceResolverStrategy must have parameterless constructor
        config.put(SchemaResolverProperties.ARTIFACT_RESOLVER_STRATEGY, CustomArtifactReferenceResolverStrategy.class);

        // Create and configure Avro serializer
        try (Serializer<GenericRecord> serializer = new AvroSerializer<>()) {
            serializer.configure(config);

            // At this point, the Avro serializer is configured and ready to use for serialization.
            // The ArtifactReferenceResolverStrategy will be used to determine the specific
            // ArtifactReference for a given data record, enabling the system to locate the correct schema in the registry.
        }
    }

    /**
     * A custom implementation of ArtifactReferenceResolverStrategy that uses the destination name
     * from the record's metadata as the artifactId.
     */
    private static class CustomArtifactReferenceResolverStrategy implements ArtifactReferenceResolverStrategy<Object, String> {

        /**
         * Determines whether to load and provide the parsed schema to the
         * {@link #artifactReference(Record, ParsedSchema)} method.
         *
         * @return false, indicating that the schema should not be loaded and passed.
         */
        @Override
        public boolean loadSchema() {
            return false;
        }

        /**
         * Resolves and returns the {@link ArtifactReference} for a given record.
         * This implementation uses the destination name from the record's metadata as the artifactId.
         *
         * @param data The record for which to resolve the {@link ArtifactReference}.
         * @param parsedSchema The schema of the record being resolved. This is not used in this implementation.
         * @return The {@link ArtifactReference} with null groupId and the destination name as artifactId.
         * @throws IllegalStateException if the provided record is null.
         */
        @Override
        public ArtifactReference artifactReference(Record<Object> data, ParsedSchema<String> parsedSchema) {
            if (data == null) {
                throw new IllegalStateException("Record is null");
            }

            SerdeRecord<Object> customRecord = (SerdeRecord<Object>) data;
            ArtifactReferenceBuilder builder = new ArtifactReferenceBuilder();
            return builder.groupId(null).artifactId(customRecord.getMetadata().getDestinationName()).build();
        }
    }
}
