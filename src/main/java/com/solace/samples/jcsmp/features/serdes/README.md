This package will contain Java samples that demonstrate how to Serialize and Deserialize Messages with the Solace JCSMP API.

### Solace Schema Registry
For information about how to deploy and configure the Solace Schema Registry, please refer to our documentation here:
https://docs.solace.com/Schema-Registry/schema-registry-overview.htm

### Uploading Schemas

To upload a schema in the Solace Schema Registry, follow these steps:

1. Begin by logging into an account with write access and click on the "Create Artifact" button.

2. Leave the Group Id field empty.

     ### Avro Schema
    - **Artifact Id**: Use a unique identifier for each schema:
      - For `user.avsc`, use `solace/samples/avro`
      - For `clock-in-out.avsc`, use `solace/samples/clock-in-out/avro`
      - For `create-user.avsc`, use `solace/samples/create-user/avro`
      - For `create-user-response.avsc`, use `solace/samples/create-user-response/avro`
      - **Type**: Select `Avro Schema`.

     ### JSON Schema
    - **Artifact Id**: Use a unique identifier for each schema:
      - For `user.json`, use `solace/samples/json`
      - For `clock-in-out.json`, use `solace/samples/clock-in-out/json`
      - For `create-user.json`, use `solace/samples/create-user/json`
      - For `create-user-response.json`, use `solace/samples/create-user-response/json`
      - **Type**: Select `JSON Schema`.

>   **Note:** Each schema must be uploaded separately with its own unique Artifact Id to avoid conflicts.

After setting the Artifact ID and Type, follow these steps:

4. Click the "Next" button to proceed.

5. You can skip the Artifact Metadata section as it's not required. Simply press "Next" to continue.

6. On the Version Content Page, leave the version set to auto, or if preferred, enter a specific value of your choice.

### For Avro Schema:
7. On the Version Content Page, upload the appropriate schema file from the `jcsmp/src/main/resources/avro-schema/` directory:
   - When using Artifact Id `solace/samples/avro`, upload `user.avsc`
   - When using Artifact Id `solace/samples/clock-in-out/avro`, upload `clock-in-out.avsc`
   - When using Artifact Id `solace/samples/create-user/avro`, upload `create-user.avsc`
   - When using Artifact Id `solace/samples/create-user-response/avro`, upload `create-user-response.avsc`

### For JSON Schema:
7. On the Version Content Page, upload the appropriate schema file from the `jcsmp/src/main/resources/json-schema/` directory:
   - When using Artifact Id `solace/samples/json`, upload `user.json`
   - When using Artifact Id `solace/samples/clock-in-out/json`, upload `clock-in-out.json`
   - When using Artifact Id `solace/samples/create-user/json`, upload `create-user.json`
   - When using Artifact Id `solace/samples/create-user-response/json`, upload `create-user-response.json`

8. Click "Next" to move forward.

9. The Version Metadata is not necessary and can be skipped.

10. Finally, click the "Create" button to complete the process.

NOTE: The registry URL, username, and password can be customized by setting environment variables. 
If not set, the application will use default values. 
To override the defaults, set the following environment variables before running the application:
The values shown below are the default settings. Modify these as needed for your specific registry configuration.
```shell
export REGISTRY_URL="http://localhost:8081/apis/registry/v3"
export REGISTRY_USERNAME="sr-readonly"
export REGISTRY_PASSWORD="roPassword"
```

# Enabling Network-Level Debug Logging

The Solace Schema Registry Serdes provider uses the Vert.x framework for making REST API calls to the Schema Registry. Vert.x has its own logging abstraction layer and will automatically detect and delegate to a logging backend on the classpath. The order of preference is SLF4J, Log4j2, and then Java Util Logging (JUL). For more information on Vert.x logging, take a look at the documentation https://vertx.io/docs/vertx-core/java/#_logging.

This project is configured to use **Log4j2**. To enable detailed network-level logging for SERDES samples, you can use JVM system variables (recommended) or modify the log4j2.xml file.

## Using JVM System Variables

The easiest way to enable network logging is with JVM system variables:

**Using JAVA_OPTS environment variable**
```bash
# Enable detailed network logging for Schema Registry communication
export JAVA_OPTS="-Dschema_registry_network_log_level=debug"

# Then run your SERDES samples normally
cd build/staged
bin/HelloWorldJCSMPJsonSchemaSerde brokerUrl default default
```

## Alternative - Modify log4j2.xml File

If you prefer to permanently enable network logging, you can modify the `src/dist/config/log4j2.xml` file:

1. Open the `src/dist/config/log4j2.xml` file.

2. Find the io.netty logger and change the level from `warn` to `debug`:

   ```xml
   <!-- Change this line -->
   <Logger name="io.netty" level="${sys:schema_registry_network_log_level:-debug}" additivity="false">
   ```