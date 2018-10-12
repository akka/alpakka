/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.cryptography.javadsl;

//import akka.NotUsed;
//import akka.actor.ActorSystem;
//import akka.stream.ActorMaterializer;
//import akka.stream.javadsl.Sink;
//import akka.stream.javadsl.Source;
//import akka.util.ByteString;
//import akka.util.ByteString$;
//import org.junit.BeforeClass;
//import org.junit.Test;
//import akka.stream.alpakka.cryptography.javadsl.CryptographicFlows.*;
//import javax.crypto.Cipher;
//import javax.crypto.KeyGenerator;
//import javax.crypto.SecretKey;
//import java.util.Arrays;
//import java.util.List;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.CompletionStage;
//import java.util.stream.Collectors;

public class ExampleTest {
//
//    static ActorSystem system;
//    static ActorMaterializer materializer;
//
//    @BeforeClass
//    public static void setupMaterializer() {
//        //#init-actor-system
//        final ActorSystem system = ActorSystem.create();
//        final ActorMaterializer materializer = ActorMaterializer.create(system);
//
//        ExampleTest.system = system;
//        ExampleTest.materializer = materializer;
//    }
//
//    @Test
//    public void asymmetricEncryptionDecryptionExampleTest() throws Exception {
//        //#init-keys-and-ciphers
//
//        KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
//        SecretKey generatedKey = keyGenerator.generateKey();
//
//        Cipher encryptionCipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
//        encryptionCipher.init(Cipher.ENCRYPT_MODE, generatedKey);
//
//        Cipher decryptionCipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
//        decryptionCipher.init(Cipher.DECRYPT_MODE, generatedKey);
//
//        List<String> toEncrypt = Arrays.asList("Some", "string", "for", "you");
//        List<ByteString> byteStringsToEncrypt = toEncrypt.stream().map(ByteString$.MODULE$::apply).collect(Collectors.toList());
//
//
//        Source<ByteString, NotUsed> sourceOfUnencryptedData = Source.from(byteStringsToEncrypt);
//
//        //#java-encrypt
//        Source<ByteString, NotUsed> sourceOfEncryptedData = sourceOfUnencryptedData.via(new CryptographicFlows().cipherFlow(encryptionCipher));
//
//        Source<ByteString, NotUsed> sourceOfDecryptedData = sourceOfEncryptedData.via(new CryptographicFlows().cipherFlow(decryptionCipher));
//
//        Sink<ByteString, CompletionStage<ByteString>> concatSink = Sink.fold(ByteString.empty(), ByteString::concat);
//
//
//        CompletableFuture<ByteString> resultOfDecryption = sourceOfDecryptedData
//                .runWith(concatSink, materializer).whenComplete((r, error) -> assertEquals(r.utf8String(), toEncrypt.) )
//                .toCompletableFuture();
//
//        assertEquals(resultOfDecryption.get().utf8String(), "Somestringforyou");
//        //#java-asymmetric
//    }
}
