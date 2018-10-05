/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.cryptography.javadsl;

import akka.NotUsed;
import akka.stream.javadsl.Flow;
import akka.util.ByteString;
import akka.stream.alpakka.cryptography.impl.Flows$;

import javax.crypto.Cipher;

public class CryptographicFlows {
    public Flow<ByteString, ByteString, NotUsed> encrypt(Cipher cipher) {
        return Flows$.MODULE$.encrypt(cipher).asJava();
    }
}
