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
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.DeliveryMode;
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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * AvroSerdesReplier
 * This class demonstrates how to use Solace JCSMP API with Avro serialization and deserialization for Request-Reply messaging.
 * It connects to a Solace message broker, receives request messages, deserializes them using AvroDeserializer,
 * creates a response, serializes it using AvroSerializer, and sends it back to the requestor.
 */
public class AvroSerdesReplier {
    public static final String REGISTRY_URL = Util.getEnv("REGISTRY_URL","http://localhost:8081/apis/registry/v3");
    public static final String REGISTRY_USERNAME = Util.getEnv("REGISTRY_USERNAME", "sr-readonly");
    public static final String REGISTRY_PASSWORD = Util.getEnv("REGISTRY_PASSWORD", "roPassword");

    // Topic for receiving requests
    public static final String REQUEST_TOPIC = "solace/samples/create-user/avro";

    /**
     * The main method that demonstrates the Solace JCSMP API usage with Avro deserialization for Request-Reply.
     *
     * @param args Command line arguments: <host:port> <message-vpn> <client-username> [password]
     * @throws Exception If any error occurs during execution
     */
    public static void main(String[] args) throws Exception {
        // Check if the required command line arguments are provided
        if (args.length < 3) {
            System.out.printf("Usage: %s <host:port> <message-vpn> <client-username> [password]%n%n", AvroSerdesReplier.class.getName());
            System.exit(-1);
        }

        // Extract connection details from command line arguments
        String host = args[0];
        String vpn = args[1];
        String clientUsername = args[2];
        String password = args.length > 3 ? args[3] : null;

        // Create and configure Avro serializer for replies and deserializer for requests
        try (Serializer<GenericRecord> serializer = new AvroSerializer<>();
             Deserializer<GenericRecord> deserializer = new AvroDeserializer<>()) {
            
            serializer.configure(getConfig());
            deserializer.configure(getConfig());

            // Create a JCSMP session and set up the request topic
            JCSMPSession session = createSession(host, vpn, clientUsername, password);
            Topic requestTopic = JCSMPFactory.onlyInstance().createTopic(REQUEST_TOPIC);
            
            // Create a message producer for sending replies
            XMLMessageProducer producer = createProducer(session);
            
            // Create a request handler
            RequestHandler handler = new RequestHandler(producer, serializer);
            
            // Create a message consumer with using Consumed.with approach
            XMLMessageConsumer consumer = session.getMessageConsumer(
                    Consumed.with(deserializer, 
                        handler::handleRequest,
                        handler::handleDeserializationException,
                        handler::handleException
                    )
            );
            
            // Add subscription to the request topic
            session.addSubscription(requestTopic);
            
            // Start the consumer to receive messages
            consumer.start();

            System.out.println("AvroSerdesReplier started. Waiting for requests ... Press enter to exit");
            System.in.read();
            
            // Clean up resources
            consumer.close();
            session.closeSession();
        }
    }
    
    /**
     * RequestHandler class that handles incoming request messages.
     */
    static class RequestHandler {
        private final XMLMessageProducer producer;
        private final Serializer<GenericRecord> serializer;
        
        /**
         * Constructor for the RequestHandler.
         *
         * @param producer The XMLMessageProducer for sending replies
         * @param serializer The serializer for serializing reply messages
         */
        public RequestHandler(XMLMessageProducer producer, Serializer<GenericRecord> serializer) {
            this.producer = producer;
            this.serializer = serializer;
        }
        
        /**
         * Handles the request message by processing it and sending a reply.
         *
         * @param message The received message
         * @param createUserRequest The deserialized request object
         */
        public void handleRequest(BytesXMLMessage message, GenericRecord createUserRequest) {
            try {
                System.out.println("Received request message, processing...");
                System.out.printf("Deserialized request: %s%n", createUserRequest);
                
                // Extract user information from the request
                String name = createUserRequest.get("name").toString();
                String email = createUserRequest.get("email").toString();
                
                System.out.printf("Processing user creation request for: %s (%s)%n", name, email);
                
                // Create a response with a generated ID
                GenericRecord createUserResponse = AvroSerdesReplier.initEmptyResponseRecord();
                String userId = generateUserId();
                createUserResponse.put("id", userId);
                
                System.out.printf("Created user with ID: %s%n", userId);
                
                // Create a reply message
                BytesMessage replyMessage = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
                replyMessage.setDeliveryMode(DeliveryMode.DIRECT);
                
                // Get the reply destination from the request message
                if (message.getReplyTo() == null) {
                    System.err.println("Error: Request message does not contain a replyTo destination");
                    return;
                }
                
                // Serialize the response
                SerdeMessage.serialize(serializer, message.getReplyTo(), replyMessage, createUserResponse);
                
                // Send the reply
                producer.sendReply(message, replyMessage);
                System.out.println("Sent reply message");
            } catch (Exception e) {
                System.err.println("Error in message processing: " + e.getMessage());
                e.printStackTrace();
            }
        }
        
        /**
         * Handles JCSMP exceptions.
         *
         * @param exception The JCSMP exception
         */
        public void handleException(JCSMPException exception) {
            System.err.println("Consumer received exception: " + exception);
            exception.printStackTrace();
        }
        
        /**
         * Handles deserialization exceptions.
         *
         * @param message The message that caused the exception
         * @param exception The deserialization exception
         */
        public void handleDeserializationException(BytesXMLMessage message, Exception exception) {
            System.err.println("Deserialization error: " + exception.getMessage());
            System.err.println("Message dump: " + message.dump());
            exception.printStackTrace();
        }
        
        /**
         * Generates a unique user ID.
         *
         * @return A unique user ID string
         */
        private String generateUserId() {
            return UUID.randomUUID().toString().substring(0, 8);
        }
    }
    
    /**
     * Initializes an empty Avro GenericRecord based on the "create-user-response.avsc" schema for responses.
     *
     * @return An empty GenericRecord for the CreateUserResponse schema
     * @throws IOException If there's an error reading the schema file
     */
    private static GenericRecord initEmptyResponseRecord() throws IOException {
        try (InputStream rawSchema = HelloWorldJCSMPAvroSerde.class.getResourceAsStream("/avro-schema/create-user-response.avsc")) {
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
            @Override public void handleErrorEx(Object o, JCSMPException e, long l) {
                System.err.println("Producer error: " + e);
            }
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
