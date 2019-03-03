/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package elastic;

// #sample
import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;

import akka.stream.alpakka.elasticsearch.ElasticsearchWriteSettings;
import akka.stream.alpakka.elasticsearch.WriteMessage;
import akka.stream.alpakka.elasticsearch.javadsl.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import akka.stream.alpakka.slick.javadsl.Slick;
import akka.stream.alpakka.slick.javadsl.SlickRow;
import akka.stream.alpakka.slick.javadsl.SlickSession;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.concurrent.CompletionStage;

// #sample

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import playground.ElasticSearchEmbedded;
import playground.elastic.ElasticsearchMock;

public class FetchUsingSlickAndStreamIntoElasticInJava {

  public static void main(String[] args) {
    FetchUsingSlickAndStreamIntoElasticInJava me = new FetchUsingSlickAndStreamIntoElasticInJava();
    me.run();
  }

  public
  // #sample

  static class Movie { // (3)
    public final int id;
    public final String title;
    public final String genre;
    public final double gross;

    @JsonCreator
    public Movie(
        @JsonProperty("id") int id,
        @JsonProperty("title") String title,
        @JsonProperty("genre") String genre,
        @JsonProperty("gross") double gross) {
      this.id = id;
      this.title = title;
      this.genre = genre;
      this.gross = gross;
    }
  }

  // #sample

  void run() {
    ElasticsearchClusterRunner runner = ElasticSearchEmbedded.startElasticInstance();

    // #sample
    ActorSystem system = ActorSystem.create();
    Materializer materializer = ActorMaterializer.create(system);

    SlickSession session = SlickSession.forConfig("slick-h2-mem"); // (1)
    system.registerOnTermination(session::close);

    // #sample
    ElasticsearchMock.populateDataForTable(session, materializer);

    // #sample
    RestClient elasticSearchClient =
        RestClient.builder(new HttpHost("localhost", 9201)).build(); // (4)

    final ObjectMapper objectToJsonMapper = new ObjectMapper(); // (5)

    final CompletionStage<Done> done =
        Slick.source( // (6)
                session,
                "SELECT * FROM MOVIE",
                (SlickRow row) ->
                    new Movie(row.nextInt(), row.nextString(), row.nextString(), row.nextDouble()))
            .map(movie -> WriteMessage.createIndexMessage(String.valueOf(movie.id), movie)) // (8)
            .runWith(
                ElasticsearchSink.create( // (9)
                    "movie",
                    "boxoffice",
                    ElasticsearchWriteSettings.Default(),
                    elasticSearchClient,
                    objectToJsonMapper),
                materializer);

    done.thenRunAsync(
            () -> {
              try {
                elasticSearchClient.close(); // (10)
              } catch (IOException ignored) {
                ignored.printStackTrace();
              }
            },
            system.dispatcher())
        // #sample
        .thenRunAsync(
            () -> {
              try {
                runner.close();
              } catch (IOException ignored) {
                ignored.printStackTrace();
              }
              runner.clean();
              system.terminate();
            });
  }
}
