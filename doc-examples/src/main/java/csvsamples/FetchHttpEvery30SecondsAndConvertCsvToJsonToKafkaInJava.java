/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package csvsamples;

// #sample
import akka.Done;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;

import akka.http.javadsl.Http;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.MediaRanges;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.model.headers.Accept;

import akka.japi.Pair;

import akka.kafka.ConsumerSettings;
import akka.kafka.ProducerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;

import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.csv.javadsl.CsvParsing;
import akka.stream.alpakka.csv.javadsl.CsvToMap;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.StringWriter;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

// #sample
import playground.KafkaEmbedded;

public class FetchHttpEvery30SecondsAndConvertCsvToJsonToKafkaInJava {

  public static void main(String[] args) throws Exception {
    FetchHttpEvery30SecondsAndConvertCsvToJsonToKafkaInJava me =
        new FetchHttpEvery30SecondsAndConvertCsvToJsonToKafkaInJava();
    me.run();
  }

  // #helper
  final HttpRequest httpRequest =
      HttpRequest.create(
              "https://www.nasdaq.com/screening/companies-by-name.aspx?exchange=NASDAQ&render=download")
          .withHeaders(Collections.singletonList(Accept.create(MediaRanges.ALL_TEXT)));

  private Source<ByteString, ?> extractEntityData(HttpResponse httpResponse) {
    if (httpResponse.status() == StatusCodes.OK) {
      return httpResponse.entity().getDataBytes();
    } else {
      return Source.failed(new RuntimeException("illegal response " + httpResponse));
    }
  }

  private Map<String, String> cleanseCsvData(Map<String, ByteString> map) {
    Map<String, String> out = new HashMap<>(map.size());
    map.forEach(
        (key, value) -> {
          if (!key.isEmpty()) out.put(key, value.utf8String());
        });
    return out;
  }

  private final JsonFactory jsonFactory = new JsonFactory();

  private String toJson(Map<String, String> map) throws Exception {
    StringWriter sw = new StringWriter();
    JsonGenerator generator = jsonFactory.createGenerator(sw);
    generator.writeStartObject();
    map.forEach(
        (key, value) -> {
          try {
            generator.writeStringField(key, value);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
    generator.writeEndObject();
    generator.close();
    return sw.toString();
  }
  // #helper

  private void run() throws Exception {
    ActorSystem system = ActorSystem.create();
    Materializer materializer = ActorMaterializer.create(system);

    int kafkaPort = 19000;
    KafkaEmbedded.start(kafkaPort);
    ProducerSettings<String, String> kafkaProducerSettings =
        ProducerSettings.create(system, new StringSerializer(), new StringSerializer())
            .withBootstrapServers(String.format("localhost:%d", kafkaPort));

    Http http = Http.get(system);

    Pair<Cancellable, CompletionStage<Done>> stagePair =
        // #sample
        Source.tick(
                Duration.ofSeconds(1),
                Duration.ofSeconds(30),
                httpRequest) // : HttpRequest             (1)
            .mapAsync(1, http::singleRequest) // : HttpResponse            (2)
            .flatMapConcat(this::extractEntityData) // : ByteString              (3)
            .via(CsvParsing.lineScanner()) // : List<ByteString>        (4)
            .via(CsvToMap.toMap()) // : Map<String, ByteString> (5)
            .map(this::cleanseCsvData) // : Map<String, String>     (6)
            .map(this::toJson) // : String                  (7)
            .map(
                elem ->
                    new ProducerRecord<String, String>(
                        "topic1", elem) // : Kafka ProducerRecord    (8)
                )
            .toMat(Producer.plainSink(kafkaProducerSettings), Keep.both())
            .run(materializer);
    // #sample

    ConsumerSettings<String, String> kafkaConsumerSettings =
        ConsumerSettings.create(system, new StringDeserializer(), new StringDeserializer())
            .withBootstrapServers(String.format("localhost:%d", kafkaPort))
            .withGroupId("topic1")
            .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    Consumer.DrainingControl<Done> control =
        Consumer.atMostOnceSource(kafkaConsumerSettings, Subscriptions.topics("topic1"))
            .map(ConsumerRecord::value)
            .toMat(Sink.foreach(System.out::println), Keep.both())
            .mapMaterializedValue(Consumer::createDrainingControl)
            .run(materializer);

    Cancellable tick = stagePair.first();
    CompletionStage<Done> streamCompletion = stagePair.second();

    Thread.sleep(20 * 1000);
    tick.cancel();

    Executor ec = Executors.newSingleThreadExecutor();

    streamCompletion
        .thenApplyAsync(done -> control.drainAndShutdown(ec))
        .thenAccept(
            done -> {
              KafkaEmbedded.stop();
              system.terminate();
            });
  }
}
