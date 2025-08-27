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
import com.solace.serdes.Deserializer;
import com.solace.serdes.Serializer;
import com.solace.serdes.avro.AvroDeserializer;
import com.solace.serdes.avro.AvroSerializer;
import com.solace.serdes.common.resolver.config.SchemaResolverProperties;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.serialization.Consumed;
import com.solacesystems.jcsmp.serialization.SerdeMessage;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * HelloWorldJCSMPAvroSerde
 * This class demonstrates the usage of Solace JCSMP API with Avro serialization and deserialization.
 * It connects to a Solace message broker, publishes a message using Avro serialization, and consumes
 * the message using Avro deserialization.
 */
public class HelloWorldJCSMPAvroSerde {
    public static final String REGISTRY_URL = Util.getEnv("REGISTRY_URL","http://localhost:8081/apis/registry/v3");
    public static final String REGISTRY_USERNAME =  Util.getEnv("REGISTRY_USERNAME", "sr-readonly");
    public static final String REGISTRY_PASSWORD =  Util.getEnv("REGISTRY_PASSWORD", "roPassword");

    public static final String TOPIC = "solace/samples/clock-in-out/avro";

    /**
     * The main method that demonstrates the Solace JCSMP API usage with Avro serialization/deserialization.
     *
     * @param args Command line arguments: <host:port> <message-vpn> <client-username> [password]
     * @throws Exception If any error occurs during execution
     */
    public static void main(String[] args) throws Exception {
        // Check if the required command line arguments are provided
        if (args.length < 3) {
            System.out.printf("Usage: %s <host:port> <message-vpn> <client-username> [password]%n%n", HelloWorldJCSMPAvroSerde.class.getName());
            System.exit(-1);
        }

        // Extract connection details from command line arguments
        String host = args[0];
        String vpn = args[1];
        String clientUsername = args[2];
        String password = null;
        if (args.length > 3) {
            password = args[3];
        }

        // Create a latch to synchronize the main thread with the message consumer
        CountDownLatch latch = new CountDownLatch(1);

        // Create and configure Avro serializer and deserializer
        try (Serializer<GenericRecord> serializer = new AvroSerializer<>();
             Deserializer<GenericRecord> deserializer = new AvroDeserializer<>()) {

            serializer.configure(getConfig());
            deserializer.configure(getConfig());

            // Create a JCSMP session and set up the topic
            JCSMPSession session = createSession(host, vpn, clientUsername, password);
            Topic topic = JCSMPFactory.onlyInstance().createTopic(TOPIC);
            session.addSubscription(topic);

            // Create a message producer
            XMLMessageProducer producer = createProducer(session);

            // Set up the message consumer with a deserialization callback
            XMLMessageConsumer cons = session.getMessageConsumer(Consumed.with(deserializer, (msg, genericRecord) -> {
                System.out.printf("Got ClockInOut record: %s%n", genericRecord);
                System.out.printf("Employee %s clocked in/out at store %s in region %s at %s%n",
                        genericRecord.get("employee_id"),
                        genericRecord.get("store_id"),
                        genericRecord.get("region_code"),
                        genericRecord.get("datetime"));
                latch.countDown(); // Signal the main thread that a message has been received
            }, (msg, deserializationException) -> {
                System.out.printf("Got exception: %s%n", deserializationException);
                System.out.printf("But still have access to the message: %s%n", msg.dump());
                latch.countDown();
            }, jcsmpException -> {
                System.out.printf("Got exception: %s%n", jcsmpException);
                latch.countDown();
            }));
            cons.start();

            // Create and populate a ClockInOut GenericRecord with sample data
            GenericRecord clockInOut = initEmptyClockInOutRecord();
            clockInOut.put("region_code", "NA-WEST");
            clockInOut.put("store_id", "STORE-001");
            clockInOut.put("employee_id", "EMP-12345");
            Instant instant = Instant.parse("2025-01-20T15:30:00Z");
            clockInOut.put("datetime", instant.toEpochMilli());

            // Serialize and send the message
            BytesMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
            SerdeMessage.serialize(serializer, topic, msg, clockInOut);
            System.out.printf("Sending ClockInOut Message:%n%s%n", msg.dump());
            producer.send(msg, topic);

            // Wait for the consumer to receive the message
            latch.await();

            session.closeSession();
        } // Auto-close serializer and deserializer
    }

    /**
     * Initializes an empty Avro GenericRecord based on the "clock-in-out.avsc" schema.
     *
     * @return An empty GenericRecord
     * @throws IOException If there's an error reading the schema file
     */
    private static GenericRecord initEmptyClockInOutRecord() throws IOException {
        try (InputStream rawSchema = HelloWorldJCSMPAvroSerde.class.getResourceAsStream("/avro-schema/clock-in-out.avsc")) {
            Schema schema = new SchemaParser().parse(rawSchema).mainSchema();
            return new GenericData.Record(schema);
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
     * Returns a configuration map for the Avro serializer and deserializer.
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
