/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package elastic;

// #imports
import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.actor.Terminated;
import akka.kafka.CommitterSettings;
import akka.kafka.ConsumerSettings;
import akka.kafka.ProducerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Committer;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.elasticsearch.ElasticsearchSourceSettings;
import akka.stream.alpakka.elasticsearch.ElasticsearchWriteSettings;
import akka.stream.alpakka.elasticsearch.WriteMessage;
import akka.stream.alpakka.elasticsearch.javadsl.ElasticsearchFlow;
import akka.stream.alpakka.elasticsearch.javadsl.ElasticsearchSource;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
// #imports

public class KafkaToElasticInJava {

  private static final Logger log = LoggerFactory.getLogger(KafkaToElasticInJava.class);
  private final String elasticsearchAddress;
  private final String kafkaBootstrapServers;

  private final String topic = "movies-to-elasticsearch";
  private final String groupId = "docs-group";

  private final String indexName = "movies";

  public KafkaToElasticInJava(String elasticsearchAddress, String kafkaBootstrapServers) {
    this.elasticsearchAddress = elasticsearchAddress;
    this.kafkaBootstrapServers = kafkaBootstrapServers;
  }

  public
  // #es-setup

  // Type in Elasticsearch (2)
  static class Movie {
    public final int id;
    public final String title;

    @JsonCreator
    public Movie(@JsonProperty("id") int id, @JsonProperty("title") String title) {
      this.id = id;
      this.title = title;
    }

    @Override
    public String toString() {
      return "Movie(" + id + ", title=" + title + ")";
    }
  }

  // Jackson conversion setup (3)
  final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
  final ObjectWriter movieWriter = mapper.writerFor(Movie.class);
  final ObjectReader movieReader = mapper.readerFor(Movie.class);

  // #es-setup

  private ActorSystem actorSystem;
  private Materializer materializer;
  private RestClient elasticsearchClient;

  private Consumer.DrainingControl<Done> readFromKafkaToEleasticsearch() {
    // #kafka-setup

    // configure Kafka consumer (1)
    ConsumerSettings<Integer, String> kafkaConsumerSettings =
        ConsumerSettings.create(actorSystem, new IntegerDeserializer(), new StringDeserializer())
            .withBootstrapServers(kafkaBootstrapServers)
            .withGroupId(groupId)
            .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .withStopTimeout(Duration.ofSeconds(5));
    // #kafka-setup

    // #flow

    Consumer.DrainingControl<Done> control =
        Consumer.committableSource(kafkaConsumerSettings, Subscriptions.topics(topic)) // (5)
            .asSourceWithContext(cm -> cm.committableOffset()) // (6)
            .map(cm -> cm.record())
            .map(
                consumerRecord -> { // (7)
                  Movie movie = movieReader.readValue(consumerRecord.value());
                  return WriteMessage.createUpsertMessage(String.valueOf(movie.id), movie);
                })
            .via(
                ElasticsearchFlow.createWithContext(
                    indexName,
                    "_doc",
                    ElasticsearchWriteSettings.create(),
                    elasticsearchClient,
                    mapper)) // (8)
            .map(
                writeResult -> { // (9)
                  writeResult
                      .getError()
                      .ifPresent(
                          errorJson -> {
                            throw new RuntimeException(
                                "Elasticsearch update failed "
                                    + writeResult.getErrorReason().orElse(errorJson));
                          });
                  return NotUsed.notUsed();
                })
            .asSource() // (10)
            .map(pair -> pair.second())
            .toMat(Committer.sink(CommitterSettings.create(actorSystem)), Keep.both()) // (11)
            .mapMaterializedValue(Consumer::createDrainingControl) // (12)
            .run(materializer);
    // #flow
    return control;
  }

  private CompletionStage<Terminated> run() throws Exception {
    actorSystem = ActorSystem.create();
    materializer = ActorMaterializer.create(actorSystem);
    // #es-setup

    // Elasticsearch client setup (4)
    elasticsearchClient = RestClient.builder(HttpHost.create(elasticsearchAddress)).build();
    // #es-setup

    List<Movie> movies = Arrays.asList(new Movie(23, "Psycho"), new Movie(423, "Citizen Kane"));
    CompletionStage<Done> writing = writeToKafka(movies);
    writing.toCompletableFuture().get(10, TimeUnit.SECONDS);

    Consumer.DrainingControl<Done> control = readFromKafkaToEleasticsearch();
    TimeUnit.SECONDS.sleep(5);
    CompletionStage<Done> copyingFinished = control.drainAndShutdown(actorSystem.dispatcher());
    copyingFinished.toCompletableFuture().get(10, TimeUnit.SECONDS);
    CompletionStage<List<Movie>> reading = readFromElasticsearch(elasticsearchClient);

    return reading.thenCompose(
        ms -> {
          ms.forEach(m -> System.out.println("read " + m));
          try {
            elasticsearchClient.close();
          } catch (IOException e) {
            log.error(e.toString(), e);
          }
          actorSystem.terminate();
          return actorSystem.getWhenTerminated();
        });
  }

  private CompletionStage<Done> writeToKafka(List<Movie> movies) {
    ProducerSettings<Integer, String> kafkaProducerSettings =
        ProducerSettings.create(actorSystem, new IntegerSerializer(), new StringSerializer())
            .withBootstrapServers(kafkaBootstrapServers);

    CompletionStage<Done> producing =
        Source.from(movies)
            .map(
                movie -> {
                  log.debug("producing {}", movie);
                  String json = movieWriter.writeValueAsString(movie);
                  return new ProducerRecord<>(topic, movie.id, json);
                })
            .runWith(Producer.plainSink(kafkaProducerSettings), materializer);
    producing.thenAccept(s -> log.info("Producing finished"));
    return producing;
  }

  private CompletionStage<List<Movie>> readFromElasticsearch(RestClient elasticsearchClient) {
    CompletionStage<List<Movie>> reading =
        ElasticsearchSource.typed(
                indexName,
                "_doc",
                "{\"match_all\": {}}",
                ElasticsearchSourceSettings.create(),
                elasticsearchClient,
                Movie.class)
            .map(readResult -> readResult.source())
            .runWith(Sink.seq(), materializer);
    reading.thenAccept(
        non -> {
          log.info("Reading finished");
        });
    return reading;
  }

  public static void main(String[] args) throws Exception {
    ElasticsearchContainer elasticsearchContainer =
        new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch-oss:6.4.3");
    elasticsearchContainer.start();
    String elasticsearchAddress = elasticsearchContainer.getHttpHostAddress();

    KafkaContainer kafka = new KafkaContainer("5.1.2"); // contains Kafka 2.1.x
    kafka.start();
    String kafkaBootstrapServers = kafka.getBootstrapServers();

    CompletionStage<Terminated> run =
        new KafkaToElasticInJava(elasticsearchAddress, kafkaBootstrapServers).run();

    run.thenAccept(
        res -> {
          kafka.stop();
          elasticsearchContainer.stop();
        });
  }
}
