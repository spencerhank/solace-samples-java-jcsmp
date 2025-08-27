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

import com.solace.samples.jcsmp.features.serdes.util.Util;
import com.solace.samples.jcsmp.features.serdes.util.WaitForEnterThread;
import com.solace.samples.serdes.jsonschema.CreateUser;
import com.solace.samples.serdes.jsonschema.CreateUserResponse;
import com.solace.serdes.Deserializer;
import com.solace.serdes.Serializer;
import com.solace.serdes.common.resolver.config.SchemaResolverProperties;
import com.solace.serdes.jsonschema.JsonSchemaDeserializer;
import com.solace.serdes.jsonschema.JsonSchemaSerializer;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPRequestTimeoutException;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.Requestor;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.serialization.SerdeMessage;

import java.util.HashMap;
import java.util.Map;

/**
 * JsonSchemaSerdesRequestor
 * This class demonstrates how to use Solace JCSMP API with JsonSchema serialization and deserialization for Request-Reply messaging.
 * It connects to a Solace message broker, serializes a request message using JsonSchemaSerializer, sends it to a topic,
 * waits for a reply message and then once received, deserializes the reply message using JsonSchemaDeserializer.
 */
public class JsonSchemaSerdesRequestor {
    public static final String REGISTRY_URL = Util.getEnv("REGISTRY_URL","http://localhost:8081/apis/registry/v3");
    public static final String REGISTRY_USERNAME = Util.getEnv("REGISTRY_USERNAME", "sr-readonly");
    public static final String REGISTRY_PASSWORD = Util.getEnv("REGISTRY_PASSWORD", "roPassword");

    // Topic for sending requests using destination ID strategy
    public static final String REQUEST_TOPIC = "solace/samples/create-user/json";
    public static final String REPLY_TOPIC = "solace/samples/create-user-response/json";
    
    // Timeout for waiting for a reply (in milliseconds)
    private static final int REQUEST_TIMEOUT_MS = 5000;

    /**
     * The main method that demonstrates the Solace JCSMP API usage with JsonSchema serialization and deserialization for Request-Reply.
     *
     * @param args Command line arguments: <host:port> <message-vpn> <client-username> [password]
     * @throws Exception If any error occurs during execution
     */
    public static void main(String[] args) throws Exception {
        // Check if the required command line arguments are provided
        if (args.length < 3) {
            System.out.printf("Usage: %s <host:port> <message-vpn> <client-username> [password]%n%n", JsonSchemaSerdesRequestor.class.getName());
            System.exit(-1);
        }

        // Extract connection details from command line arguments
        String host = args[0];
        String vpn = args[1];
        String clientUsername = args[2];
        String password = args.length > 3 ? args[3] : null;

        // Create and configure JsonSchema serializer for requests and deserializer for replies
        try (Serializer<CreateUser> serializer = new JsonSchemaSerializer<>();
             Deserializer<CreateUserResponse> deserializer = new JsonSchemaDeserializer<>()) {
            
            serializer.configure(getConfig());
            deserializer.configure(getConfig());

            // Create a JCSMP session and set up the request topic
            JCSMPSession session = createSession(host, vpn, clientUsername, password);

            // This will have the session create the producer and consumer required
            // by the Requestor used below.
            createProducer(session);
            XMLMessageConsumer consumer = session.getMessageConsumer((XMLMessageListener)null);
            consumer.start();

            // Create a topic for the request to be sent onto
            Topic requestTopic = JCSMPFactory.onlyInstance().createTopic(REQUEST_TOPIC);
            // Create a topic for the reply to be sent onto
            Topic replyTopic = JCSMPFactory.onlyInstance().createTopic(REPLY_TOPIC);

            // Add subscription to the replytopic
            session.addSubscription(replyTopic);
            
            // Create a requestor for request-reply messaging
            Requestor requestor = session.createRequestor();
            
            // Create and populate a POJO with sample data for the request
            CreateUser userRequest = new CreateUser();
            
            WaitForEnterThread exitListener = new WaitForEnterThread();
            exitListener.start();

            while (!exitListener.isDone()) {
                // Update request message
                userRequest.setName("John Doe");
                userRequest.setEmail("support@solace.com");

                // Create a message for the request
                BytesMessage requestMsg = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
                requestMsg.setDeliveryMode(DeliveryMode.DIRECT);
                requestMsg.setReplyTo(replyTopic);
                
                // Serialize the request message
                SerdeMessage.serialize(serializer, requestTopic, requestMsg, userRequest);
                
                System.out.printf("Sending Request: %s%n", userRequest);
                
                try {
                    // Send the request and wait for a reply
                    BytesXMLMessage replyMsg = requestor.request(requestMsg, REQUEST_TIMEOUT_MS, requestTopic);
                    
                    // Deserialize the reply message
                    CreateUserResponse userResponse = SerdeMessage.deserialize(deserializer, replyMsg);
                    
                    System.out.printf("Received Reply: %s%n", userResponse);
                    
                    // Extract and display the ID from the response
                    String userId = userResponse.getId();
                    
                    System.out.printf("User created with ID: %s%n", userId);
                    
                } catch (JCSMPRequestTimeoutException e) {
                    System.out.println("Request timed out after " + REQUEST_TIMEOUT_MS + " ms");
                }

                Thread.sleep(1000); // Limit send rate
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
            @Override public void handleErrorEx(Object o, JCSMPException e, long l) {
                System.err.println("Producer error: " + e);
            }
        });
    }

    /**
     * Returns a configuration map for the JsonSchema serializer and deserializer.
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
