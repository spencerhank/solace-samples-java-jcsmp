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
import com.solace.serdes.Serializer;
import com.solace.serdes.avro.AvroSerializer;
import com.solace.serdes.common.resolver.config.SchemaResolverProperties;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.serialization.SerdeMessage;

import java.util.HashMap;
import java.util.Map;

/**
 * AvroSerializeProducerSpecificRecord
 * This class demonstrates how to use Solace JCSMP API with Avro serialization to produce messages using specific records.
 * It connects to a Solace message broker, serializes a message using Avro, and publishes it to a topic.
 */
public class AvroSerializeProducerSpecificRecord {
    public static final String REGISTRY_URL = Util.getEnv("REGISTRY_URL","http://localhost:8081/apis/registry/v3");
    public static final String REGISTRY_USERNAME =  Util.getEnv("REGISTRY_USERNAME", "sr-readonly");
    public static final String REGISTRY_PASSWORD =  Util.getEnv("REGISTRY_PASSWORD", "roPassword");

    public static final String TOPIC = "solace/samples/avro";

    /**
     * The main method that demonstrates the Solace JCSMP API usage with Avro serialization.
     *
     * @param args Command line arguments: <host:port> <message-vpn> <client-username> [password]
     * @throws Exception If any error occurs during execution
     */
    public static void main(String[] args) throws Exception {
        // Check if the required command line arguments are provided
        if (args.length < 3) {
            System.out.printf("Usage: %s <host:port> <message-vpn> <client-username> [password]%n%n", AvroSerializeProducerSpecificRecord.class.getName());
            System.exit(-1);
        }

        // Extract connection details from command line arguments
        String host = args[0];
        String vpn = args[1];
        String clientUsername = args[2];
        String password = args.length > 3 ? args[3] : null;

        // Create and configure Avro serializer
        try (Serializer<User> serializer = new AvroSerializer<>()) {
            serializer.configure(getConfig());

            // Create a JCSMP session and set up the topic
            JCSMPSession session = createSession(host, vpn, clientUsername, password);
            Topic topic = JCSMPFactory.onlyInstance().createTopic(TOPIC);

            // Create a message producer
            XMLMessageProducer producer = createProducer(session);

            // Create and populate a User specific record with sample data
            User user = new User();
            user.setName("John Doe");
            user.setId("-1");
            user.setEmail("support@solace.com");
            BytesMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);

            WaitForEnterThread exitListener = new WaitForEnterThread();
            exitListener.start();

            int index = 0;
            while (!exitListener.isDone()) {
                //update message
                user.setId(String.format("%d", index));

                // Serialize and send the message
                SerdeMessage.serialize(serializer, topic, msg, user);
                producer.send(msg, topic);
                System.out.printf("Sending Message: %s%n", user);

                index++;
                Thread.sleep(100); //limit send rate
            }

            exitListener.join();
            session.closeSession();
        }
    }

    /**
     * Creates and returns an XMLMessageProducer for the given JCSMP session.
     *
     * @param session The JCSMP session
     * @return An XMLMessageProducer
     * @throws JCSMPException If there's an error creating the producer
     */
    private static XMLMessageProducer createProducer(JCSMPSession session) throws JCSMPException {
        return session.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
            @Override public void responseReceivedEx(Object o) {}
            @Override public void handleErrorEx(Object o, JCSMPException e, long l) {}
        });
    }

    /**
     * Returns a configuration map for the Avro serializer.
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
