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
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import static org.junit.Assert.*;

public class ExampleTest {

    static ActorSystem system;
    static ActorMaterializer materializer;

    @BeforeClass
    public static void setupMaterializer() {
        //#init-client
        final ActorSystem system = ActorSystem.create();
        final ActorMaterializer materializer = ActorMaterializer.create(system);
        //#init-client
        ExampleTest.system = system;
        ExampleTest.materializer = materializer;
    }

    @Test
    public void symmetricEncryptionDecryptionExampleTest() throws Exception {
        KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
        SecretKey randomKey = keyGenerator.generateKey();

        List<String> toEncrypt = Arrays.asList("Some", "string", "for", "you");
        List<ByteString> byteStringsToEncrypt = toEncrypt.stream().map(s -> ByteString$.MODULE$.apply(s)).collect(Collectors.toList());


        Source<ByteString, NotUsed> sourceOfUnencryptedData = Source.from(byteStringsToEncrypt);
        Source<ByteString, NotUsed> sourceOfEncryptedData = sourceOfUnencryptedData.via(CryptographicFlows.symmetricEncryption(randomKey));
        Source<ByteString, NotUsed> sourceOfDecryptedData = sourceOfEncryptedData.via(CryptographicFlows.symmetricDecryption(randomKey));
        Sink<ByteString, CompletionStage<ByteString>> concatSink = Sink.<ByteString, ByteString>fold(ByteString.empty(), (acc, nxt) -> acc);


        CompletionStage<ByteString> resultOfDecryption = sourceOfDecryptedData
                 .runWith(concatSink, materializer).whenComplete((r, error) -> assertEquals(r.utf8String(), "Somestringforyou") );

    }
}
