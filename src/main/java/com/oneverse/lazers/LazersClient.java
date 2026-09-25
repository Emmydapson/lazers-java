package com.oneverse.lazers;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/** Server-side client for Lazers Signal Protocol v1. Project API keys must never be used in browser or mobile code. */
public final class LazersClient implements AutoCloseable {
  public static final String PROTOCOL_VERSION = "1";
  private static final String SCHEMA_VERSION = "2026-07-09";
  private static final int MAX_STRING = 10_000, MAX_KEYS = 100, MAX_DEPTH = 6;
  private static final Set<String> SEVERITIES = Set.of("info", "notice", "warning", "error", "critical");
  private static final Set<String> ACTOR_TYPES = Set.of("user", "customer", "account", "merchant", "tenant", "organization", "other");
  private static final Set<String> INVALID_RELEASES = Set.of("git-sha", "sha", "unknown", "replace-me", "placeholder", "undefined", "null");

  @FunctionalInterface public interface Transport {
    int post(URI uri, Map<String, String> headers, byte[] body, Duration timeout) throws Exception;
  }
  /** Implement on a Throwable only when its code is intentionally safe to transmit. */
  public interface CodedThrowable { String getLazersCode(); }

  public static Builder builder() { return new Builder(); }
  public static final class Builder {
    private String apiKey, endpoint, projectId, environment, serviceName, release;
    private boolean enabled = true;
    private int maxQueueSize = 100, maxRetries = 5;
    private Duration retryBaseDelay = Duration.ofMillis(250), timeout = Duration.ofSeconds(5);
    private Transport transport;
    public Builder apiKey(String v) { apiKey=v; return this; } public Builder endpoint(String v) { endpoint=v; return this; }
    public Builder projectId(String v) { projectId=v; return this; } public Builder environment(String v) { environment=v; return this; }
    public Builder serviceName(String v) { serviceName=v; return this; } public Builder release(String v) { release=v; return this; }
    public Builder enabled(boolean v) { enabled=v; return this; } public Builder maxQueueSize(int v) { maxQueueSize=v; return this; }
    public Builder maxRetries(int v) { maxRetries=v; return this; } public Builder retryBaseDelay(Duration v) { retryBaseDelay=v; return this; }
    public Builder timeout(Duration v) { timeout=v; return this; } public Builder transport(Transport v) { transport=v; return this; }
    public Builder httpClient(HttpClient client) { transport = new HttpTransport(Objects.requireNonNull(client)); return this; }
    public LazersClient build() { return new LazersClient(this); }
  }

  /** Optional capture context. Values are normalized and copied before asynchronous delivery. */
  public static final class Context {
    private Map<String,Object> metadata, actor, resource; private List<?> tags, labels, affectedUsers, relatedEntities;
    private String severity, environment, correlationId, fingerprint, provider, eventName, sourceType, traceId, occurredAt, release;
    public Context metadata(Map<String,Object> v){metadata=v;return this;} public Context actor(Map<String,Object> v){actor=v;return this;}
    public Context resource(Map<String,Object> v){resource=v;return this;} public Context tags(List<?> v){tags=v;return this;}
    public Context labels(List<?> v){labels=v;return this;} public Context affectedUsers(List<?> v){affectedUsers=v;return this;}
    public Context relatedEntities(List<?> v){relatedEntities=v;return this;} public Context severity(String v){severity=v;return this;}
    public Context environment(String v){environment=v;return this;} public Context correlationId(String v){correlationId=v;return this;}
    public Context fingerprint(String v){fingerprint=v;return this;} public Context provider(String v){provider=v;return this;}
    public Context eventName(String v){eventName=v;return this;} public Context sourceType(String v){sourceType=v;return this;}
    public Context traceId(String v){traceId=v;return this;} public Context occurredAt(String v){occurredAt=v;return this;}
    public Context release(String v){release=v;return this;}
  }

  /** Input for captureSignal; category defaults from type, or custom for unknown types. */
  public static final class SignalInput {
    private final String title; private String type="custom.event", eventCategory, severity="info"; private Map<String,Object> payload=Map.of(); private Context context=new Context();
    public SignalInput(String title) { this.title=title; }
    public SignalInput type(String v){type=v;return this;} public SignalInput eventCategory(String v){eventCategory=v;return this;}
    public SignalInput severity(String v){severity=v;return this;} public SignalInput payload(Map<String,Object> v){payload=v;return this;}
    public SignalInput context(Context v){context=v;return this;}
  }

  private static final class Item { final Map<String,Object> signal; int attempts; Item(Map<String,Object> s){signal=s;} }
  private final String apiKey, endpoint, projectId, environment, serviceName, release;
  private final boolean enabled; private final int maxQueueSize, maxRetries; private final Duration retryBaseDelay, timeout; private final Transport transport;
  private final ArrayDeque<Item> queue = new ArrayDeque<>(); private final Object monitor = new Object(); private final ScheduledExecutorService executor;
  private boolean closed, draining;

  private LazersClient(Builder b) {
    apiKey=requireConfig("apiKey", b.apiKey); endpoint=requireConfig("endpoint", b.endpoint); projectId=requireConfig("projectId", b.projectId);
    environment=requireConfig("environment", b.environment); serviceName=requireConfig("serviceName", b.serviceName); release=validRelease(b.release);
    enabled=b.enabled; maxQueueSize=Math.max(1,b.maxQueueSize); maxRetries=Math.max(1,b.maxRetries);
    retryBaseDelay=b.retryBaseDelay == null || b.retryBaseDelay.isNegative() ? Duration.ofMillis(250) : b.retryBaseDelay;
    timeout=b.timeout == null || b.timeout.isNegative() || b.timeout.isZero() ? Duration.ofSeconds(5) : b.timeout;
    transport=b.transport == null ? new HttpTransport(HttpClient.newBuilder().connectTimeout(timeout).build()) : b.transport;
    ThreadFactory threads = r -> { Thread t=new Thread(r,"lazers-delivery"); t.setDaemon(true); return t; };
    executor=Executors.newSingleThreadScheduledExecutor(threads);
  }

  public void captureError(Throwable error) { captureError(error, new Context()); }
  public void captureError(Throwable error, Context context) { Context c=copyContext(context); Map<String,Object> p=new LinkedHashMap<>(); p.put("error", serializeError(error,0)); capture("", "error.detected", "error", "error", p, c, errorMessage(error)); }
  public void captureMessage(String message) { captureMessage(message, new Context()); }
  public void captureMessage(String message, Context context) { Context c=copyContext(context); Map<String,Object> p=new LinkedHashMap<>(); p.put("message",message); capture("", "custom.event", "message", "info", p, c, message); }
  public void captureJobFailure(String jobName, Throwable error) { captureJobFailure(jobName,error,new Context()); }
  public void captureJobFailure(String jobName, Throwable error, Context context) { Context c=copyContext(context); if(c.resource==null)c.resource=Map.of("type","job","id",jobName,"name",jobName); if(blank(c.eventName))c.eventName="job.failure"; Map<String,Object> p=new LinkedHashMap<>();p.put("error",serializeError(error,0));p.put("jobName",jobName);capture("","job.failed","job_failure","error",p,c,"Job failed: "+jobName); }
  public void captureWebhookFailure(String provider, Throwable error) { captureWebhookFailure(provider,error,new Context()); }
  public void captureWebhookFailure(String provider, Throwable error, Context context) { Context c=copyContext(context);c.provider=provider;if(blank(c.sourceType))c.sourceType="custom";if(blank(c.eventName))c.eventName="webhook.failure";Map<String,Object> p=new LinkedHashMap<>();p.put("error",serializeError(error,0));p.put("provider",provider);capture("","integration.failed","webhook_failure","error",p,c,"Webhook failed: "+provider); }
  public void captureSignal(SignalInput input) { Objects.requireNonNull(input,"input"); capture("",input.type,input.eventCategory,input.severity,input.payload,copyContext(input.context),input.title); }

  private void capture(String ignored, String type, String category, String defaultSeverity, Map<String,Object> payload, Context c, String title) {
    if(!enabled || closed) return;
    try { enqueue(normalize(type, blank(category)?categoryFor(type):category, blank(c.severity)?defaultSeverity:c.severity, payload, c, title)); } catch (RuntimeException ignoredFailure) { /* telemetry isolation */ }
  }
  private void enqueue(Map<String,Object> signal) { synchronized(monitor) { if(closed)return; queue.addLast(new Item(signal)); while(queue.size()>maxQueueSize)queue.removeFirst(); if(!draining){draining=true;executor.execute(this::drain);} monitor.notifyAll(); } }
  private void drain() { while(true) { Item item; synchronized(monitor){item=queue.peekFirst();if(item==null){draining=false;monitor.notifyAll();return;}} if(send(item)) { synchronized(monitor){if(queue.peekFirst()==item)queue.removeFirst();monitor.notifyAll();} } else return; } }
  private boolean send(Item item) {
    try { int status=transport.post(URI.create(endpoint.replaceAll("/+$","")+"/signals/ingest"),Map.of("Content-Type","application/json","x-lazers-api-key",apiKey),json(item.signal).getBytes(StandardCharsets.UTF_8),timeout); if(status>=200&&status<300 || !retryable(status)) return true; } catch(Exception ignored) { }
    item.attempts++; if(item.attempts>=maxRetries)return true;
    long delay=Math.min(retryBaseDelay.toMillis()*(1L<<Math.min(item.attempts-1,16)),30_000L);
    executor.schedule(this::drain,delay,TimeUnit.MILLISECONDS); return false;
  }
  public void flush() { if(!enabled)return; synchronized(monitor){if(!queue.isEmpty()&&!draining){draining=true;executor.execute(this::drain);} while(!queue.isEmpty()&&!closed){try{monitor.wait(25);}catch(InterruptedException e){Thread.currentThread().interrupt();return;}}} }
  @Override public void close() { flush(); synchronized(monitor){closed=true;monitor.notifyAll();} executor.shutdown(); try{executor.awaitTermination(timeout.toMillis(),TimeUnit.MILLISECONDS);}catch(InterruptedException e){Thread.currentThread().interrupt();} executor.shutdownNow(); }
  public int getQueueSize() { synchronized(monitor){return queue.size();} }

  private static Context copyContext(Context input) { if(input==null)return new Context(); Context c=new Context(); c.metadata=input.metadata;c.actor=input.actor;c.resource=input.resource;c.tags=input.tags;c.labels=input.labels;c.affectedUsers=input.affectedUsers;c.relatedEntities=input.relatedEntities;c.severity=input.severity;c.environment=input.environment;c.correlationId=input.correlationId;c.fingerprint=input.fingerprint;c.provider=input.provider;c.eventName=input.eventName;c.sourceType=input.sourceType;c.traceId=input.traceId;c.occurredAt=input.occurredAt;c.release=input.release;return c; }
  private static String errorMessage(Throwable error) { return error == null ? "<null>" : (error.getMessage() == null ? error.getClass().getName() : error.getMessage()); }
  private Map<String,Object> normalize(String type,String category,String severity,Map<String,Object> payload,Context c,String title) {
    String provider=blank(c.provider)?"lazers-agent-sdk":c.provider, sourceType=blank(c.sourceType)?"agent_sdk":c.sourceType;
    Map<String,Object> cleanPayload=asMap(sanitize(payload,0)); Map<String,Object> metadata=asMap(sanitize(c.metadata==null?Map.of():c.metadata,0)); String effectiveRelease=validRelease(c.release==null?release:c.release); if(effectiveRelease!=null)metadata.put("release",effectiveRelease); metadata.put("serviceName",serviceName);
    Map<String,Object> error=cleanPayload.get("error") instanceof Map<?,?> ? asMap(cleanPayload.get("error")) : null;
    String fingerprint=blank(c.fingerprint)?fingerprint(category,type,provider,c.eventName,c.resource,error,title):stable(c.fingerprint,256);
    Map<String,Object> source=new LinkedHashMap<>();source.put("type",sourceType);source.put("provider",required(provider,80));if(!blank(c.eventName))source.put("eventName",required(c.eventName,160));
    Map<String,Object> result=new LinkedHashMap<>(); result.put("protocolVersion",PROTOCOL_VERSION);result.put("schemaVersion",SCHEMA_VERSION);result.put("projectId",required(projectId,200));result.put("environment",required(c.environment==null?environment:c.environment,80));result.put("eventCategory",category);result.put("type",type);result.put("severity",SEVERITIES.contains(severity)?severity:"info");result.put("title",required(title,240));result.put("occurredAt",blank(c.occurredAt)?DateTimeFormatter.ISO_INSTANT.format(Instant.now()):c.occurredAt);result.put("fingerprint",fingerprint);result.put("traceId",blank(c.traceId)?"exec_"+UUID.randomUUID():required(c.traceId,160));result.put("source",source);result.put("privacy",Map.of("classification","internal","containsPersonalData",false,"containsSecrets",false,"aiProcessingAllowed",true,"retention","standard"));result.put("payload",cleanPayload);result.put("metadata",metadata);result.put("status","received");
    if(!blank(c.correlationId))result.put("correlationId",c.correlationId); Map<String,Object> actor=actor(c.actor);if(actor!=null)result.put("actor",actor); putSafe(result,"resource",c.resource);putSafe(result,"tags",c.tags);putSafe(result,"labels",c.labels);putSafe(result,"affectedUsers",c.affectedUsers);putSafe(result,"relatedEntities",c.relatedEntities); return result;
  }
  private static void putSafe(Map<String,Object> into,String key,Object value){if(value!=null)into.put(key,sanitize(value,0));}
  public static Map<String,Object> serializeError(Throwable error) { return serializeError(error,0); }
  private static Map<String,Object> serializeError(Throwable error,int depth){Map<String,Object> out=new LinkedHashMap<>();if(error==null){out.put("name","NilError");out.put("message","<null>");return out;}out.put("name",error.getClass().getSimpleName());out.put("message",truncate(error.getMessage()==null?error.getClass().getName():error.getMessage(),MAX_STRING));StringWriter sw=new StringWriter();error.printStackTrace(new PrintWriter(sw));out.put("stack",truncate(sw.toString(),MAX_STRING));if(error instanceof CodedThrowable coded && !blank(coded.getLazersCode()))out.put("code",truncate(coded.getLazersCode(),160));if(error.getCause()!=null&&depth<3)out.put("cause",serializeError(error.getCause(),depth+1));return asMap(sanitize(out,0));}
  private static Object sanitize(Object value,int depth){if(value==null||value instanceof Boolean||value instanceof Number)return value;if(value instanceof String s)return truncate(s,MAX_STRING);if(depth>=MAX_DEPTH)return "[TRUNCATED]";if(value instanceof Map<?,?> map){Map<String,Object> out=new LinkedHashMap<>();int n=0;for(var e:map.entrySet()){if(n++>=MAX_KEYS)break;String key=truncate(String.valueOf(e.getKey()),160);out.put(key,sensitive(key)?"[REDACTED]":sanitize(e.getValue(),depth+1));}return out;}if(value instanceof Collection<?> list){List<Object> out=new ArrayList<>();int n=0;for(Object v:list){if(n++>=MAX_KEYS)break;out.add(sanitize(v,depth+1));}return out;}if(value.getClass().isArray()){List<Object> out=new ArrayList<>();for(int i=0;i<Math.min(Array.getLength(value),MAX_KEYS);i++)out.add(sanitize(Array.get(value,i),depth+1));return out;}return "[UNSUPPORTED_OBJECT]";}
  private static boolean sensitive(String key){String n=key.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]","");for(String token:List.of("password","passwd","secret","token","authorization","cookie","apikey","cardnumber","card","cvv","pin","refreshtoken","accesstoken","body","rawbody","webhookpayload"))if(n.contains(token))return true;return false;}
  private static String fingerprint(String category,String type,String provider,String event,Map<String,Object> resource,Map<String,Object> error,String title){List<String> parts=new ArrayList<>();for(String s:List.of(category,type,provider,event==null?"":event,stringValue(resource,"type"),stringValue(resource,"id"),stringValue(error,"name"),error==null?title:stringValue(error,"message")))if(!blank(s))parts.add(stable(s,512));return sha256(String.join("|",parts));}
  private static String stringValue(Map<String,Object> m,String key){return m!=null&&m.get(key) instanceof String s?s:"";} private static String sha256(String v){try{byte[] b=MessageDigest.getInstance("SHA-256").digest(v.getBytes(StandardCharsets.UTF_8));StringBuilder s=new StringBuilder();for(byte x:b)s.append(String.format("%02x",x));return s.toString();}catch(Exception e){throw new IllegalStateException(e);}}
  private static Map<String,Object> actor(Map<String,Object> v){if(v==null||!(v.get("id") instanceof String id)||blank(id))return null;Map<String,Object> out=new LinkedHashMap<>();String t=v.get("type") instanceof String x&&ACTOR_TYPES.contains(x)?x:"other";out.put("type",t);out.put("id",required(id,200));if(v.get("label") instanceof String label&&!blank(label))out.put("label",truncate(label.trim(),240));return out;}
  @SuppressWarnings("unchecked") private static Map<String,Object> asMap(Object value){if(!(value instanceof Map<?,?> m))return new LinkedHashMap<>();Map<String,Object> out=new LinkedHashMap<>();m.forEach((k,v)->out.put(String.valueOf(k),v));return out;}
  private static String categoryFor(String type){return switch(type){case "error.detected"->"error";case "job.failed"->"job_failure";case "integration.failed"->"webhook_failure";default->"custom";};} private static boolean retryable(int s){return s==408||s==429||s>=500&&s<=599;} private static String requireConfig(String name,String value){if(blank(value))throw new IllegalArgumentException("lazers: "+name+" is required");return value.trim();} private static boolean blank(String v){return v==null||v.trim().isEmpty();} private static String required(String v,int n){String s=truncate(v==null?"":v.trim(),n);return s.isEmpty()?"unknown":s;} private static String stable(String v,int n){return truncate(v.trim().toLowerCase(Locale.ROOT).replaceAll("\\s+"," "),n);} private static String truncate(String v,int n){return v.length()<=n?v:v.substring(0,n);} private static String validRelease(String v){if(blank(v))return null;String s=v.trim();return INVALID_RELEASES.contains(s.toLowerCase(Locale.ROOT))?null:s;}
  private static final class HttpTransport implements Transport { private final HttpClient client; HttpTransport(HttpClient c){client=c;} public int post(URI uri,Map<String,String> h,byte[] body,Duration timeout)throws Exception{HttpRequest.Builder b=HttpRequest.newBuilder(uri).timeout(timeout).POST(HttpRequest.BodyPublishers.ofByteArray(body));h.forEach(b::header);return client.send(b.build(),HttpResponse.BodyHandlers.discarding()).statusCode();} }
  private static String json(Object v){if(v==null)return "null";if(v instanceof String s)return '"'+s.replace("\\","\\\\").replace("\"","\\\"").replace("\n","\\n").replace("\r","\\r").replace("\t","\\t")+'"';if(v instanceof Boolean||v instanceof Number)return String.valueOf(v);if(v instanceof Map<?,?> m){List<String> p=new ArrayList<>();for(var e:m.entrySet())p.add(json(String.valueOf(e.getKey()))+":"+json(e.getValue()));return "{"+String.join(",",p)+"}";}if(v instanceof Collection<?> c){List<String> p=new ArrayList<>();for(Object x:c)p.add(json(x));return "["+String.join(",",p)+"]";}return json("[UNSUPPORTED_OBJECT]");}
}