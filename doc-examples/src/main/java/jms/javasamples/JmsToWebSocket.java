/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package jms.javasamples;

// #sample

import akka.Done;
import akka.actor.ActorSystem;
import akka.http.javadsl.Http;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.model.ws.Message;
import akka.http.javadsl.model.ws.TextMessage;
import akka.http.javadsl.model.ws.WebSocketRequest;
import akka.http.javadsl.model.ws.WebSocketUpgradeResponse;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.jms.JmsConsumerSettings;
import akka.stream.alpakka.jms.JmsProducerSettings;
import akka.stream.alpakka.jms.javadsl.JmsConsumer;
import akka.stream.alpakka.jms.javadsl.JmsConsumerControl;
import akka.stream.alpakka.jms.javadsl.JmsProducer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import playground.ActiveMqBroker;
import playground.WebServer;
import scala.concurrent.ExecutionContext;

import javax.jms.ConnectionFactory;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

// #sample

public class JmsToWebSocket {

  public static void main(String[] args) throws Exception {
    JmsToWebSocket me = new JmsToWebSocket();
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
            JmsConsumerSettings.create(system, connectionFactory)
                .withBufferSize(10)
                .withQueue("test"));

    Flow<Message, Message, CompletionStage<WebSocketUpgradeResponse>> webSocketFlow = // (2)
        http.webSocketClientFlow(WebSocketRequest.create("ws://localhost:8080/webSocket/ping"));

    int parallelism = 4;
    Pair<Pair<JmsConsumerControl, CompletionStage<WebSocketUpgradeResponse>>, CompletionStage<Done>>
        pair =
            jmsSource // : String
                .map(
                    s -> {
                      Message msg = TextMessage.create(s);
                      return msg;
                    }) // : Message           (3)
                .viaMat(webSocketFlow, Keep.both()) // : Message           (4)
                .mapAsync(parallelism, this::wsMessageToString) // : String            (5)
                .map(s -> "client received: " + s) // : String            (6)
                .toMat(Sink.foreach(System.out::println), Keep.both()) //                    (7)
                .run(materializer);
    // #sample
    JmsConsumerControl runningSource = pair.first().first();
    CompletionStage<WebSocketUpgradeResponse> wsUpgradeResponse = pair.first().second();
    CompletionStage<Done> streamCompletion = pair.second();

    wsUpgradeResponse
        .thenApply(
            upgrade -> {
              if (upgrade.response().status() == StatusCodes.SWITCHING_PROTOCOLS) {
                return "WebSocket established";
              } else {
                throw new RuntimeException("Connection failed: " + upgrade.response().status());
              }
            })
        .thenAccept(System.out::println);

    Thread.sleep(2 * 1000);
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

  // #sample

  /** Convert potentially chunked WebSocket Message to a string. */
  private CompletionStage<String> wsMessageToString(Message msg) {
    if (msg.isText()) {
      TextMessage tMsg = msg.asTextMessage();
      if (tMsg.isStrict()) {
        return CompletableFuture.completedFuture(tMsg.getStrictText());
      } else {
        CompletionStage<List<String>> strings =
            tMsg.getStreamedText().runWith(Sink.seq(), materializer);
        return strings.thenApply(list -> String.join("", list));
      }
    } else {
      return CompletableFuture.completedFuture(msg.toString());
    }
  }
  // #sample

}
