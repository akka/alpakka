# FTP Connector

The FTP connector provides Akka Stream sources to connect to FTP, FTPs and SFTP servers. Currently, two kinds of sources are provided:

* one for browsing or traversing the server recursively and, 
* another for retrieving files as a stream of bytes.

## Artifacts

sbt
:   @@@vars
    ```scala
    libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-ftp" % "$version$"
    ```
    @@@

Maven
:   @@@vars
    ```xml
    <dependency>
      <groupId>com.lightbend.akka</groupId>
      <artifactId>akka-stream-alpakka-ftp_$scalaBinaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@vars
    ```gradle
    dependencies {
      compile group: "com.lightbend.akka", name: "akka-stream-alpakka-ftp_$scalaBinaryVersion$", version: "$version$"
    }
    ```
    @@@

## Usage

### Configuring the connection settings

In order to establish a connection with the remote server, you need to provide a specialized version of a @scaladoc[RemoteFileSettings](akka.stream.alpakka.ftp.RemoteFileSettings) instance. It's specialized as it depends on the kind of server you're connecting to: FTP, FTPs or SFTP.

Scala
: @@snip (../../../../ftp/src/test/scala/akka/stream/alpakka/ftp/BaseFtpSpec.scala) { #create-settings }

Java
: @@snip (../../../../ftp/src/test/java/akka/stream/alpakka/ftp/FtpStageTest.java) { #create-settings }

The configuration above will create an anonymous connection with a remote FTP server in passive mode. For both FTPs and SFTP servers, you will need to provide the specialized versions of these settings: @scaladoc[FtpsSettings](akka.stream.alpakka.ftp.RemoteFileSettings$$FtpsSettings) or @scaladoc[SftpSettings](akka.stream.alpakka.ftp.RemoteFileSettings$$SftpSettings)
respectively.

For non-anonymous connection, please provide an instance of @scaladoc[NonAnonFtpCredentials](akka.stream.alpakka.ftp.FtpCredentials$$NonAnonFtpCredentials) instead.

For connection using a private key, please provide an instance of @scaladoc[SftpIdentity](akka.stream.alpakka.ftp.SftpIdentity) to @scaladoc[SftpSettings](akka.stream.alpakka.ftp.RemoteFileSettings$$SftpSettings).

### Traversing a remote FTP folder recursively

In order to traverse a remote folder recursively, you need to use the `ls` method in the FTP API:

Scala
: @@snip (../../../../ftp/src/test/scala/akka/stream/alpakka/ftp/BaseFtpSpec.scala) { #traversing }

Java
: @@snip (../../../../ftp/src/test/java/akka/stream/alpakka/ftp/FtpStageTest.java) { #traversing }

This source will emit @scaladoc[FtpFile](akka.stream.alpakka.ftp.FtpFile) elements with no significant materialization.

For both FTPs and SFTP servers, you will need to use the `FTPs` and `SFTP` API respectively.

### Retrieving files

In order to retrieve a remote file as a stream of bytes, you need to use the `fromPath` method in the FTP API:

Scala
: @@snip (../../../../ftp/src/test/scala/akka/stream/alpakka/ftp/BaseFtpSpec.scala) { #retrieving }

Java
: @@snip (../../../../ftp/src/test/java/akka/stream/alpakka/ftp/FtpStageTest.java) { #retrieving }

This source will emit @scaladoc[ByteString](akka.util.ByteString) elements and materializes to @scaladoc[Future](scala.concurrent.Future) in Scala API and @extref[CompletionStage](java-api:java/util/concurrent/CompletionStage) in Java API of @scaladoc[IOResult](akka.stream.IOResult) when the stream finishes.

For both FTPs and SFTP servers, you will need to use the `FTPs` and `SFTP` API respectively.

### Writing files

In order to store a remote file from a stream of bytes, you need to use the `toPath` method in the FTP API:

Scala
: @@snip (../../../../ftp/src/test/scala/akka/stream/alpakka/ftp/BaseFtpSpec.scala) { #storing }

Java
: @@snip (../../../../ftp/src/test/java/akka/stream/alpakka/ftp/FtpStageTest.java) { #storing }

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
