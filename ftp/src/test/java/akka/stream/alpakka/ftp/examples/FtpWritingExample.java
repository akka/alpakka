/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp.examples;

//#storing
import akka.stream.IOResult;
import akka.stream.alpakka.ftp.FtpSettings;
import akka.stream.alpakka.ftp.javadsl.Ftp;
import akka.stream.javadsl.Sink;
import akka.util.ByteString;

import java.util.concurrent.CompletionStage;

public class FtpWritingExample {

    public Sink<ByteString, CompletionStage<IOResult>> storeToPath(String path, FtpSettings settings) throws Exception {
        return Ftp.toPath(path, settings);
    }
}
//#storing
