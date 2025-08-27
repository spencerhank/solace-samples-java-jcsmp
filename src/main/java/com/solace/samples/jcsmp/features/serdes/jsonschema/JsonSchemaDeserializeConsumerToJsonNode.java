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

package com.solace.samples.jcsmp.features.serdes.jsonschema;

import com.fasterxml.jackson.databind.JsonNode;
import com.solace.samples.jcsmp.features.serdes.util.Util;
import com.solace.samples.jcsmp.features.serdes.util.WaitForEnterThread;
import com.solace.serdes.Deserializer;
import com.solace.serdes.common.resolver.config.SchemaResolverProperties;
import com.solace.serdes.jsonschema.JsonSchemaDeserializer;
import com.solace.serdes.jsonschema.JsonSchemaValidationException;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.serialization.SerdeMessage;

import java.util.HashMap;
import java.util.Map;

/**
 * This sample demonstrates how to deserialize a JSON message payload to a JsonNode.
 * This approach is useful for handling JSON in a generic way, without needing a predefined Java class.
 */
public class JsonSchemaDeserializeConsumerToJsonNode {
    public static final String REGISTRY_URL = Util.getEnv("REGISTRY_URL","http://localhost:8081/apis/registry/v3");
    public static final String REGISTRY_USERNAME =  Util.getEnv("REGISTRY_USERNAME", "sr-readonly");
    public static final String REGISTRY_PASSWORD =  Util.getEnv("REGISTRY_PASSWORD", "roPassword");

    public static final String TOPIC = Util.getEnv("TOPIC", "solace/samples/json");

    /**
     * The main method that demonstrates the Solace JCSMP API usage with Json Schema deserialization to a JsonNode.
     *
     * @param args Command line arguments: <host:port> <message-vpn> <client-username> [password]
     * @throws Exception If any error occurs during execution
     */
    public static void main(String[] args) throws Exception {
        // Check if the required command line arguments are provided
        if (args.length < 3) {
            System.out.printf("Usage: %s <host:port> <message-vpn> <client-username> [password]%n%n", JsonSchemaDeserializeConsumerToJsonNode.class.getName());
            System.exit(-1);
        }

        // Extract connection details from command line arguments
        String host = args[0];
        String vpn = args[1];
        String clientUsername = args[2];
        String password = args.length > 3 ? args[3] : null;

        // Create and configure Json Schema deserializer
        try (Deserializer<JsonNode> deserializer = new JsonSchemaDeserializer<>()) {
            deserializer.configure(getConfig());

            // Create a JCSMP session and set up the topic
            JCSMPSession session = createSession(host, vpn, clientUsername, password);
            Topic topic = JCSMPFactory.onlyInstance().createTopic(TOPIC);

            // Create a message consumer and subscribe to the topic
            final XMLMessageConsumer consumer = session.getMessageConsumer((XMLMessageListener) null);
            session.addSubscription(topic);

            WaitForEnterThread exitListener = new WaitForEnterThread();
            exitListener.start();

            // Start the consumer and wait for a message
            consumer.start();
            while (!exitListener.isDone()) {
                // Try to receive a message with a 1-second timeout
                BytesXMLMessage msg = consumer.receive(1000);
                if (msg == null) continue;

                try {
                    // Deserialize the received message
                    // Note: The deserializer will return a generic JsonNode unless a target POJO is specified.
                    // There are two ways to specify the target class:
                    // 1. In the deserializer configuration: Set the SerdeProperties.DESERIALIZED_TYPE property.
                    // 2. In the schema: Add a property that contains the fully qualified Java class name.
                    //    - The name of this schema property is determined by JsonSchemaProperties.TYPE_PROPERTY.
                    //    - By default, its value is "javaType". For example: { "javaType": "com.my.User" }
                    //    - You can configure TYPE_PROPERTY to use a different key if your schemas follow another convention.
                    JsonNode jsonNode = SerdeMessage.deserialize(deserializer, msg);
                    System.out.println("Got message: " + jsonNode);

                } catch (JsonSchemaValidationException ve) {
                    System.out.printf("Received Message with invalid payload:%nValidation error: %s%n", ve);
                } catch (RuntimeException re) {
                    System.out.printf("Received Message with payload that can not be decoded:%nDecoding error: %s%n", re);
                }
            }

            exitListener.join();
            session.closeSession();
        }
    }

    /**
     * Returns a configuration map for the Json Schema deserializer.
     *
     * @return A Map containing configuration properties
     */
    private static Map<String, Object> getConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(SchemaResolverProperties.REGISTRY_URL, REGISTRY_URL);
        config.put(SchemaResolverProperties.AUTH_USERNAME, REGISTRY_USERNAME);
        config.put(SchemaResolverProperties.AUTH_PASSWORD, REGISTRY_PASSWORD);
        return config;
    }

    /**
     * Creates and returns a JCSMP session with the provided connection details.
     *
     * @param host The host address of the Solace message broker
     * @param vpn The message VPN name
     * @param user The client username
     * @param password The client password (optional)
     * @return A connected JCSMPSession
     * @throws JCSMPException If there's an error creating or connecting the session
     */
    private static JCSMPSession createSession(String host, String vpn, String user, String password) throws JCSMPException {
        JCSMPProperties props = new JCSMPProperties();
        props.setProperty(JCSMPProperties.HOST, host);
        props.setProperty(JCSMPProperties.VPN_NAME, vpn);
        props.setProperty(JCSMPProperties.USERNAME, user);
        if (password != null) {
            props.setProperty(JCSMPProperties.PASSWORD, password);
        }

        JCSMPSession session = JCSMPFactory.onlyInstance().createSession(props);
        session.connect();
        return session;
    }
}
