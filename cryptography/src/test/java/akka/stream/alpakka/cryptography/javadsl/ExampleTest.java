/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.cryptography.javadsl;

public class ExampleTest {
//
//    static ActorSystem system;
//    static ActorMaterializer materializer;
//
//    @BeforeClass
//    public static void setupMaterializer() {
//        //#init-client
//        final ActorSystem system = ActorSystem.create();
//        final ActorMaterializer materializer = ActorMaterializer.create(system);
//        //#init-client
//        ExampleTest.system = system;
//        ExampleTest.materializer = materializer;
//    }
//
//    @Test
//    public void symmetricEncryptionDecryptionExampleTest() throws Exception {
//        //#java-symmetric
//        KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
//        SecretKey randomKey = keyGenerator.generateKey();
//
//        List<String> toEncrypt = Arrays.asList("Some", "string", "for", "you");
//        List<ByteString> byteStringsToEncrypt = toEncrypt.stream().map(ByteString$.MODULE$::apply).collect(Collectors.toList());
//
//
//        Source<ByteString, NotUsed> sourceOfUnencryptedData = Source.from(byteStringsToEncrypt);
//        Source<ByteString, NotUsed> sourceOfEncryptedData = sourceOfUnencryptedData.via(CryptographicFlows.symmetricEncryption(randomKey));
//        Source<ByteString, NotUsed> sourceOfDecryptedData = sourceOfEncryptedData.via(CryptographicFlows.symmetricDecryption(randomKey));
//        Sink<ByteString, CompletionStage<ByteString>> concatSink = Sink.fold(ByteString.empty(), ByteString::concat);
//
//        CompletableFuture<ByteString> resultOfDecryption = sourceOfDecryptedData
//                .runWith(concatSink, materializer).whenComplete((r, error) -> assertEquals(r.utf8String(), "Somestringforyou") )
//                .toCompletableFuture();
//
//        assertEquals(resultOfDecryption.get().utf8String(), "Somestringforyou");
//        //#java-symmetric
//    }
//
//    @Test
//    public void asymmetricEncryptionDecryptionExampleTest() throws Exception {
//        //#java-asymmetric
//        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
//        KeyPair randomKeyPair = keyPairGenerator.generateKeyPair();
//
//        List<String> toEncrypt = Arrays.asList("Some", "string", "for", "you");
//        List<ByteString> byteStringsToEncrypt = toEncrypt.stream().map(ByteString$.MODULE$::apply).collect(Collectors.toList());
//
//
//        Source<ByteString, NotUsed> sourceOfUnencryptedData = Source.from(byteStringsToEncrypt);
//        Source<ByteString, NotUsed> sourceOfEncryptedData = sourceOfUnencryptedData.via(CryptographicFlows.asymmetricEncryption(randomKeyPair.getPublic()));
//        Source<ByteString, NotUsed> sourceOfDecryptedData = sourceOfEncryptedData.via(CryptographicFlows.asymmetricDecryption(randomKeyPair.getPrivate()));
//        Sink<ByteString, CompletionStage<ByteString>> concatSink = Sink.fold(ByteString.empty(), ByteString::concat);
//
//
//        CompletableFuture<ByteString> resultOfDecryption = sourceOfDecryptedData
//                .runWith(concatSink, materializer).whenComplete((r, error) -> assertEquals(r.utf8String(), "Somestringforyou") )
//                .toCompletableFuture();
//
//        assertEquals(resultOfDecryption.get().utf8String(), "Somestringforyou");
//        //#java-asymmetric
//    }
}
