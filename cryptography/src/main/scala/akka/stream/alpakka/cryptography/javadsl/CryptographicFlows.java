package akka.stream.alpakka.cryptography.javadsl;

import akka.NotUsed;
import akka.stream.alpakka.cryptography.scaladsl.CryptographicFlows$;
import akka.stream.javadsl.Flow;
import akka.util.ByteString;

import javax.crypto.SecretKey;
import java.security.PrivateKey;
import java.security.PublicKey;

public class CryptographicFlows {
    public static Flow<ByteString, ByteString, NotUsed> symmetricEncryption(SecretKey secretKey) {
        return CryptographicFlows$.MODULE$.symmetricEncryption(secretKey).asJava();
    }

    public static Flow<ByteString, ByteString, NotUsed> symmetricDencryption(SecretKey secretKey) {
        return CryptographicFlows$.MODULE$.symmetricDecryption(secretKey).asJava();
    }

    public static Flow<ByteString, ByteString, NotUsed> asymmetricEncryption(PublicKey publicKey) {
        return CryptographicFlows$.MODULE$.asymmetricEncryption(publicKey).asJava();
    }

    public static Flow<ByteString, ByteString, NotUsed> asymmetricDecryption(PrivateKey privateKey) {
        return CryptographicFlows$.MODULE$.asymmetricDecryption(privateKey).asJava();
    }
}
