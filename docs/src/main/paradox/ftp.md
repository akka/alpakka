# FTP Connector

The FTP connector provides Akka Stream sources to connect to FTP, FTPs and SFTP servers. Currently, two kinds of sources are provided:

* one for browsing or traversing the server recursively and,
* another for retrieving files as a stream of bytes.

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-ftp_$scalaBinaryVersion$
  version=$version$
}

## Usage

### Configuring the connection settings

In order to establish a connection with the remote server, you need to provide a specialized version of a @scaladoc[RemoteFileSettings](akka.stream.alpakka.ftp.RemoteFileSettings) instance. It's specialized as it depends on the kind of server you're connecting to: FTP, FTPs or SFTP.

Scala
: @@snip ($alpakka$/ftp/src/test/scala/akka/stream/alpakka/ftp/scalaExamples.scala) { #create-settings }

Java
: @@snip ($alpakka$/ftp/src/test/java/akka/stream/alpakka/ftp/examples/FtpSettingsExample.java) { #create-settings }

The configuration above will create an anonymous connection with a remote FTP server in passive mode. For both FTPs and SFTP servers, you will need to provide the specialized versions of these settings: @scaladoc[FtpsSettings](akka.stream.alpakka.ftp.RemoteFileSettings$$FtpsSettings) or @scaladoc[SftpSettings](akka.stream.alpakka.ftp.RemoteFileSettings$$SftpSettings)
respectively.

For non-anonymous connection, please provide an instance of @scaladoc[NonAnonFtpCredentials](akka.stream.alpakka.ftp.FtpCredentials$$NonAnonFtpCredentials) instead.

For connection using a private key, please provide an instance of @scaladoc[SftpIdentity](akka.stream.alpakka.ftp.SftpIdentity) to @scaladoc[SftpSettings](akka.stream.alpakka.ftp.RemoteFileSettings$$SftpSettings).

### Traversing a remote FTP folder recursively

In order to traverse a remote folder recursively, you need to use the `ls` method in the FTP API:

Scala
: @@snip ($alpakka$/ftp/src/test/scala/akka/stream/alpakka/ftp/scalaExamples.scala) { #traversing }

Java
: @@snip ($alpakka$/ftp/src/test/java/akka/stream/alpakka/ftp/examples/FtpTraversingExample.java) { #traversing }

@scala[@github[Source on Github](/ftp/src/test/scala/akka/stream/alpakka/ftp/scalaExamples.scala) { #traversing }]
@java[@github[Source on Github](/ftp/src/test/java/akka/stream/alpakka/ftp/examples/FtpTraversingExample.java) { #traversing }]


This source will emit @scaladoc[FtpFile](akka.stream.alpakka.ftp.FtpFile) elements with no significant materialization.

For both FTPs and SFTP servers, you will need to use the `FTPs` and `SFTP` API respectively.

### Retrieving files

In order to retrieve a remote file as a stream of bytes, you need to use the `fromPath` method in the FTP API:

Scala
: @@snip ($alpakka$/ftp/src/test/scala/akka/stream/alpakka/ftp/scalaExamples.scala) { #retrieving }

Java
: @@snip ($alpakka$/ftp/src/test/java/akka/stream/alpakka/ftp/examples/FtpRetrievingExample.java) { #retrieving }

@scala[@github[Source on Github](/ftp/src/test/scala/akka/stream/alpakka/ftp/scalaExamples.scala) { #retrieving }]
@java[@github[Source on Github](/ftp/src/test/java/akka/stream/alpakka/ftp/examples/FtpRetrievingExample.java) { #retrieving }]


This source will emit @scaladoc[ByteString](akka.util.ByteString) elements and materializes to @scaladoc[Future](scala.concurrent.Future) in Scala API and @extref[CompletionStage](java-api:java/util/concurrent/CompletionStage) in Java API of @scaladoc[IOResult](akka.stream.IOResult) when the stream finishes.

For both FTPs and SFTP servers, you will need to use the `FTPs` and `SFTP` API respectively.

### Writing files

In order to store a remote file from a stream of bytes, you need to use the `toPath` method in the FTP API:

Scala
: @@snip ($alpakka$/ftp/src/test/scala/akka/stream/alpakka/ftp/scalaExamples.scala) { #storing }

Java
: @@snip ($alpakka$/ftp/src/test/java/akka/stream/alpakka/ftp/examples/FtpWritingExample.java) { #storing }

@scala[@github[Source on Github](/ftp/src/test/scala/akka/stream/alpakka/ftp/scalaExamples.scala) { #storing }]
@java[@github[Source on Github](/ftp/src/test/java/akka/stream/alpakka/ftp/examples/FtpWritingExample.java) { #storing }]


This sink will consume @scaladoc[ByteString](akka.util.ByteString) elements and materializes to @scaladoc[Future](scala.concurrent.Future) in Scala API and @extref[CompletionStage](java-api:java/util/concurrent/CompletionStage) in Java API of @scaladoc[IOResult](akka.stream.IOResult) when the stream finishes.

For both FTPs and SFTP servers, you will need to use the `FTPs` and `SFTP` API respectively.

### Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to browse the code, edit and run it in sbt.

Scala
:   ```
    sbt
    > ftp/test
    ```

Java
:   ```
    sbt
    > ftp/test
    ```

@@@ warning
When using the `SFTP` API, take into account that JVM relies on `/dev/random` for random number generation by default. This might potentially block the process on some operating systems as `/dev/random` waits for a certain amount of entropy to be generated on the host machine before returning a result. In such case, please consider providing the parameter `-Djava.security.egd = file:/dev/./urandom` into the execution context. Further information can be found [here](https://www.2uo.de/myths-about-urandom/).
@@@
