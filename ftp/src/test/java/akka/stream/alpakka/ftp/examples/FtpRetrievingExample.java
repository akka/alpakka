/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp.examples;

//#retrieving
import akka.stream.IOResult;
import akka.stream.alpakka.ftp.FtpSettings;
import akka.stream.alpakka.ftp.javadsl.Ftp;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import java.util.concurrent.CompletionStage;

public class FtpRetrievingExample {

    public Source<ByteString, CompletionStage<IOResult>> retrieveFromPath(String path, FtpSettings settings) throws Exception {
        return Ftp.fromPath(path, settings);
    }
}
//#retrieving
