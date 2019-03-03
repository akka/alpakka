/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package mqtt.javasamples;

import akka.Done;
import akka.actor.ActorSystem;
import akka.japi.Creator;
import akka.japi.Pair;
import akka.stream.*;
import akka.stream.alpakka.mqtt.*;
import akka.stream.alpakka.mqtt.javadsl.MqttSink;
import akka.stream.alpakka.mqtt.javadsl.MqttSource;
import akka.stream.javadsl.*;
import akka.util.ByteString;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.IOException;
import java.io.StringWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class MqttGroupedWithin {

  public static void main(String[] args) throws Exception {
    MqttGroupedWithin me = new MqttGroupedWithin();
    me.run();
  }

  final ActorSystem system = ActorSystem.create("integration");
  final Materializer materializer = ActorMaterializer.create(system);

  // #json-mechanics

  /** Data elements sent via MQTT broker. */
  public static final class Measurement {
    public final Instant timestamp;
    public final long level;

    @JsonCreator
    public Measurement(
        @JsonProperty("timestamp") Instant timestamp, @JsonProperty("level") long level) {
      this.timestamp = timestamp;
      this.level = level;
    }
  }

  private final JsonFactory jsonFactory = new JsonFactory();

  final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
  final ObjectReader measurementReader = mapper.readerFor(Measurement.class);
  final ObjectWriter measurementWriter = mapper.writerFor(Measurement.class);

  private String asJsonArray(String fieldName, List<Object> list) throws IOException {
    StringWriter sw = new StringWriter();
    JsonGenerator generator = jsonFactory.createGenerator(sw);
    generator.writeStartObject();
    generator.writeFieldName(fieldName);
    measurementWriter.writeValues(generator).init(true).writeAll(list);
    generator.close();
    return sw.toString();
  }
  // #json-mechanics

  // #restarting
  /** Wrap a source with restart logic and exposes an equivalent materialized value. */
  <M> Source<M, CompletionStage<Done>> wrapWithAsRestartSource(
      Creator<Source<M, CompletionStage<Done>>> source) {
    // makes use of the fact that these sources materialize a CompletionStage<Done>
    CompletableFuture<Done> fut = new CompletableFuture<>();
    return RestartSource.withBackoff(
            Duration.ofMillis(100),
            Duration.ofSeconds(3),
            0.2d, // randomFactor
            5, // maxRestarts,
            () ->
                source
                    .create()
                    .mapMaterializedValue(
                        mat ->
                            mat.handle(
                                (done, exception) -> {
                                  if (done != null) {
                                    fut.complete(done);
                                  } else {
                                    fut.completeExceptionally(exception);
                                  }
                                  return fut.toCompletableFuture();
                                })))
        .mapMaterializedValue(ignore -> fut.toCompletableFuture());
  }
  // #restarting

  void run() throws Exception {
    // #flow
    final MqttConnectionSettings connectionSettings =
        MqttConnectionSettings.create(
            "tcp://localhost:1883", // (1)
            "coffee-client",
            new MemoryPersistence());

    final String topic = "coffee/level";

    MqttSubscriptions subscriptions = MqttSubscriptions.create(topic, MqttQoS.atLeastOnce()); // (2)

    Source<MqttMessage, CompletionStage<Done>> restartingMqttSource =
        wrapWithAsRestartSource( // (3)
            () ->
                MqttSource.atMostOnce(
                    connectionSettings.withClientId("coffee-control"), subscriptions, 8));

    Pair<Pair<CompletionStage<Done>, UniqueKillSwitch>, CompletionStage<Done>> completions =
        restartingMqttSource
            .viaMat(KillSwitches.single(), Keep.both()) // (4)
            .map(m -> m.payload().utf8String()) // (5)
            .map(measurementReader::readValue) // (6)
            .groupedWithin(50, Duration.ofSeconds(5)) // (7)
            .map(list -> asJsonArray("measurements", list)) // (8)
            .toMat(Sink.foreach(System.out::println), Keep.both())
            .run(materializer);
    // #flow

    // start producing messages to MQTT
    CompletionStage<Done> subscriptionInitialized = completions.first().first();
    CompletionStage<UniqueKillSwitch> producer =
        subscriptionInitialized.thenApply(
            d -> produceMessages(measurementWriter, connectionSettings, topic));

    KillSwitch listener = completions.first().second();

    CompletionStage<Done> streamCompletion = completions.second();
    streamCompletion
        .handle(
            (done, exception) -> {
              if (exception != null) {
                exception.printStackTrace();
                return null;
              } else {
                return done;
              }
            })
        .thenRun(() -> system.terminate());

    Thread.sleep(10 * 1000);

    producer.thenAccept((ks) -> ks.shutdown());
    listener.shutdown();
  }

  /** Simulate messages from MQTT by writing to topic registered in MQTT broker. */
  private UniqueKillSwitch produceMessages(
      ObjectWriter measurementWriter, MqttConnectionSettings connectionSettings, String topic) {
    List<Measurement> input =
        Arrays.asList(
            new Measurement(Instant.now(), 40),
            new Measurement(Instant.now(), 60),
            new Measurement(Instant.now(), 80),
            new Measurement(Instant.now(), 100),
            new Measurement(Instant.now(), 120));

    MqttConnectionSettings sinkSettings = connectionSettings.withClientId("coffee-supervisor");

    final Sink<MqttMessage, CompletionStage<Done>> mqttSink =
        MqttSink.create(sinkSettings, MqttQoS.atLeastOnce());
    UniqueKillSwitch killSwitch =
        Source.cycle(() -> input.iterator())
            .throttle(4, Duration.ofSeconds(1))
            .map(m -> measurementWriter.writeValueAsString(m))
            .map(s -> MqttMessage.create(topic, ByteString.fromString(s)))
            .viaMat(KillSwitches.single(), Keep.right())
            .toMat(mqttSink, Keep.left())
            .run(materializer);
    return killSwitch;
  }
}
