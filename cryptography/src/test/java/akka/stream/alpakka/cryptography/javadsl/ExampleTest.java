/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.cryptography.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import akka.util.ByteString$;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collector;

public class ExampleTest {

    static ActorSystem system;
    static ActorMaterializer materializer;

    @BeforeClass
    public static Pair<ActorSystem, ActorMaterializer> setupMaterializer() {
        //#init-client
        final ActorSystem system = ActorSystem.create();
        final ActorMaterializer materializer = ActorMaterializer.create(system);
        //#init-client
        ExampleTest.system = system;
        ExampleTest.materializer = materializer;

        return Pair.create(system, materializer);
    }

    @Test
    public void symmetricEncryptionDecryptionExampleTest() throws Exception {
        KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
        SecretKey randomKey = keyGenerator.generateKey();

        List<String> toEncrypt = Arrays.asList("Some", "string", "for", "you");
        List<ByteString> byteStringsToEncrypt = toEncrypt.stream().map(s -> new ByteString(s)).collect(Collector.of<List>);

        Source<ByteString, NotUsed> sourceOfUnencryptedData = Source.from(byteStringsToEncrypt);
        Source<ByteString, NotUsed> sourceOfEncryptedData = sourceOfUnencryptedData.via(CryptographicFlows.symmetricEncryption(randomKey));
        Source<ByteString, NotUsed> sourceOfDecryptedData = sourceOfEncryptedData.via(CryptographicFlows.symmetricDecryption(randomKey));


        val resultOfDecryption: Future[ByteString] = sourceOfDecryptedData.runWith(Sink.fold(ByteString.empty)(_ concat _))

        whenReady(resultOfDecryption){r =>
            r.utf8String shouldBe toEncrypt.mkString("")
        }

        //#simple-request
        final Future<ListTablesResult> listTablesResultFuture = client.listTables(new ListTablesRequest());
        //#simple-request
        final Duration duration = Duration.create(5, "seconds");
        ListTablesResult result = Await.result(listTablesResultFuture, duration);
    }

    @Test
    public void flow() throws Exception {
        //#flow
        Source<String, NotUsed> tableArnSource = Source
                .single(new CreateTable(new CreateTableRequest().withTableName("testTable")))
                .via(client.flow())
                .map(result -> (CreateTableResult) result)
                .map(result -> result.getTableDescription().getTableArn());
        //#flow
        final Duration duration = Duration.create(5, "seconds");
        tableArnSource.runForeach(System.out::println,materializer);
    }
}
