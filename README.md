# Lazers Java SDK

Server-side Java 17+ SDK for Lazers Signal Protocol v1.

Repository: [https://github.com/Emmydapson/lazers](https://github.com/Emmydapson/lazers)

## Coordinates

```xml
<dependency>
  <groupId>com.oneverse</groupId>
  <artifactId>lazers</artifactId>
  <version>0.1.0</version>
</dependency>
```

Or with Gradle:

```groovy
implementation 'com.oneverse:lazers:0.1.0'
```

## Quickstart

```java
import com.oneverse.lazers.LazersClient;

LazersClient client = LazersClient.builder()
    .apiKey(System.getenv("LAZERS_API_KEY"))
    .endpoint("https://api.lazers.app")
    .projectId("project_1")
    .environment("production")
    .serviceName("my-api")
    .build();

try {
    processOrder();
} catch (Exception exception) {
    client.captureError(exception);
}

// On shutdown:
client.close();
```

Use `LazersClient.Context` for metadata, actor, tags, resources, correlation ID, trace ID, release, provider, and event name. `captureSignal(new LazersClient.SignalInput(...))` supports custom and explicit event categories, including `provider_failure`.

## Spring Boot

Declare a singleton `LazersClient` `@Bean`; inject it into a `@ControllerAdvice` global exception handler and call `captureError(error, context)`. Map the authenticated principal to an actor only with a stable server-side ID/type; do not derive email or phone. Call `captureJobFailure` from scheduled jobs and `captureWebhookFailure` for provider/webhook failures. In a `@PreDestroy` method call `client.close()` to flush and stop delivery.

## Other frameworks

For Quarkus, create the client as an application-scoped bean and close it on shutdown. For Micronaut, create a singleton and register a shutdown hook. Both use the same server-only API key and helper methods; no framework package is required.

## Security

This SDK is server-side only. Never expose a project API key in Android apps, browser bundles, or frontend clients. Mobile support requires Lazers' client-safe credential/DSN model.
