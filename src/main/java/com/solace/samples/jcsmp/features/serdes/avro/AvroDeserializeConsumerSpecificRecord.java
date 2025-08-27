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

package com.solace.samples.jcsmp.features.serdes.avro;

import com.solace.samples.jcsmp.features.serdes.util.Util;
import com.solace.samples.jcsmp.features.serdes.util.WaitForEnterThread;
import com.solace.samples.serdes.avro.schema.User;
import com.solace.serdes.Deserializer;
import com.solace.serdes.avro.AvroDeserializer;
import com.solace.serdes.avro.AvroProperties;
import com.solace.serdes.common.resolver.config.SchemaResolverProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.serialization.Consumed;

import java.util.HashMap;
import java.util.Map;

/**
 * AvroDeserializeConsumerSpecificRecord
 * This class demonstrates how to use Solace JCSMP API with Avro deserialization to consume messages using specific records.
 * It connects to a Solace message broker, subscribes to a topic, and deserializes the received messages using Avro.
 */
public class AvroDeserializeConsumerSpecificRecord {
    public static final String REGISTRY_URL = Util.getEnv("REGISTRY_URL","http://localhost:8081/apis/registry/v3");
    public static final String REGISTRY_USERNAME =  Util.getEnv("REGISTRY_USERNAME", "sr-readonly");
    public static final String REGISTRY_PASSWORD =  Util.getEnv("REGISTRY_PASSWORD", "roPassword");

    public static final String TOPIC = "solace/samples/avro";

    /**
     * The main method that demonstrates the Solace JCSMP API usage with Avro deserialization.
     *
     * @param args Command line arguments: <host:port> <message-vpn> <client-username> [password]
     * @throws Exception If any error occurs during execution
     */
    public static void main(String[] args) throws Exception {
        // Check if the required command line arguments are provided
        if (args.length < 3) {
            System.out.printf("Usage: %s <host:port> <message-vpn> <client-username> [password]%n%n", AvroDeserializeConsumerSpecificRecord.class.getName());
            System.exit(-1);
        }

        // Extract connection details from command line arguments
        String host = args[0];
        String vpn = args[1];
        String clientUsername = args[2];
        String password = args.length > 3 ? args[3] : null;

        // Create and configure Avro deserializer
        try (Deserializer<User> deserializer = new AvroDeserializer<>()) {
            deserializer.configure(getConfig());

            // Create a JCSMP session and set up the topic
            JCSMPSession session = createSession(host, vpn, clientUsername, password);
            Topic topic = JCSMPFactory.onlyInstance().createTopic(TOPIC);

            WaitForEnterThread exitListener = new WaitForEnterThread();
            exitListener.start();

            // Create a message consumer with Consumed.with approach
            XMLMessageConsumer consumer = session.getMessageConsumer(Consumed.with(deserializer, 
                (msg, user) -> {
                    // Success callback
                    System.out.printf("Got message: %s%n", user);
                }, 
                (msg, deserializationException) -> {
                    // Deserialization exception callback
                    System.out.printf("Got deserialization exception: %s%n", deserializationException);
                    System.out.printf("But still have access to the message: %s%n", msg.dump());
                }, 
                jcsmpException -> {
                    // JCSMP exception callback
                    System.out.printf("Got JCSMP exception: %s%n", jcsmpException);
                }));

            session.addSubscription(topic);
            consumer.start();

            // Wait for user to press Enter to exit
            while (!exitListener.isDone()) {
                Thread.sleep(100);
            }

            exitListener.join();
            session.closeSession();
        }
    }

    /**
     * Returns a configuration map for the Avro deserializer.
     *
     * @return A Map containing configuration properties
     */
    private static Map<String, Object> getConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(SchemaResolverProperties.REGISTRY_URL, REGISTRY_URL);
        config.put(SchemaResolverProperties.AUTH_USERNAME, REGISTRY_USERNAME);
        config.put(SchemaResolverProperties.AUTH_PASSWORD, REGISTRY_PASSWORD);
        config.put(AvroProperties.RECORD_TYPE, AvroProperties.AvroRecordType.SPECIFIC_RECORD);
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
