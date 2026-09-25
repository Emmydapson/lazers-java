# Lazers Java SDK (private) v0.1.0

Server-side Java 17+ SDK for Lazers Signal Protocol v1. This module is intentionally not configured for publication.

```java
LazersClient client = LazersClient.builder()
    .apiKey("server-only-key")
    .endpoint("https://api.lazers.app")
    .projectId("project_1")
    .environment("production")
    .serviceName("my-api")
    .build();
client.captureError(exception);
```

Use `LazersClient.Context` for metadata, actor, tags, resources, correlation ID, trace ID, release, provider, and event name. `captureSignal(new LazersClient.SignalInput(...))` supports custom and explicit event categories, including `provider_failure`.

## Spring Boot

Declare a singleton `LazersClient` `@Bean`; inject it into a `@ControllerAdvice` global exception handler and call `captureError(error, context)`. Map the authenticated principal to an actor only with a stable server-side ID/type; do not derive email or phone. Call `captureJobFailure` from scheduled jobs and `captureWebhookFailure` for provider/webhook failures. In a `@PreDestroy` method call `client.close()` to flush and stop delivery.

## Other frameworks

For Quarkus, create the client as an application-scoped bean and close it on shutdown. For Micronaut, create a singleton and register a shutdown hook. Both use the same server-only API key and helper methods; no framework package is required.

## Security

This SDK is server-side only. Never expose a project API key in Android apps, browser bundles, or frontend clients. Mobile support requires Lazers' future client-safe credential/DSN model.
