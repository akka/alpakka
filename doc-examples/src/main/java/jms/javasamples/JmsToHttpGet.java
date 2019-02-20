/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package jms.javasamples;

// #sample

import akka.Done;
import akka.actor.ActorSystem;
import akka.http.javadsl.Http;
import akka.http.javadsl.model.HttpRequest;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.jms.JmsConsumerSettings;
import akka.stream.alpakka.jms.JmsProducerSettings;
import akka.stream.alpakka.jms.javadsl.JmsConsumer;
import akka.stream.alpakka.jms.javadsl.JmsConsumerControl;
import akka.stream.alpakka.jms.javadsl.JmsProducer;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import playground.ActiveMqBroker;
import playground.WebServer;
import scala.concurrent.ExecutionContext;

import javax.jms.ConnectionFactory;
import java.util.Arrays;
import java.util.concurrent.CompletionStage;

// #sample

public class JmsToHttpGet {

  public static void main(String[] args) throws Exception {
    JmsToHttpGet me = new JmsToHttpGet();
    me.run();
  }

  private final ActorSystem system = ActorSystem.create();
  private final Materializer materializer = ActorMaterializer.create(system);
  private final ExecutionContext ec = system.dispatcher();

  private void enqueue(ConnectionFactory connectionFactory, String... msgs) {
    Sink<String, ?> jmsSink =
        JmsProducer.textSink(
            JmsProducerSettings.create(system, connectionFactory).withQueue("test"));
    Source.from(Arrays.asList(msgs)).runWith(jmsSink, materializer);
  }

  private void run() throws Exception {
    ActiveMqBroker activeMqBroker = new ActiveMqBroker();
    activeMqBroker.start();

    WebServer webserver = new WebServer();
    webserver.start("localhost", 8080);

    ConnectionFactory connectionFactory = activeMqBroker.createConnectionFactory();
    enqueue(connectionFactory, "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k");
    // #sample

    final Http http = Http.get(system);

    Source<String, JmsConsumerControl> jmsSource = // (1)
        JmsConsumer.textSource(
            JmsConsumerSettings.create(system, connectionFactory).withQueue("test"));

    int parallelism = 4;
    Pair<JmsConsumerControl, CompletionStage<Done>> pair =
        jmsSource // : String
            .map(ByteString::fromString) // : ByteString   (2)
            .map(
                bs ->
                    HttpRequest.create("http://localhost:8080/hello")
                        .withEntity(bs)) // : HttpRequest  (3)
            .mapAsyncUnordered(parallelism, http::singleRequest) // : HttpResponse (4)
            .toMat(Sink.foreach(System.out::println), Keep.both()) //               (5)
            .run(materializer);
    // #sample
    Thread.sleep(5 * 1000);
    JmsConsumerControl runningSource = pair.first();
    CompletionStage<Done> streamCompletion = pair.second();

    runningSource.shutdown();
    streamCompletion.thenAccept(res -> system.terminate());
    system
        .getWhenTerminated()
        .thenAccept(
            t -> {
              webserver.stop();
              activeMqBroker.stop(ec);
            });
  }
}
