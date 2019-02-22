# FTP

### Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-file_$scala.binary.version$
  version=$project.version$
  group2=com.lightbend.akka
  artifact2=akka-stream-alpakka-ftp_$scala.binary.version$
  version2=$project.version$
}

### Example: Copy all files from an FTP server to local files

- list FTP server contents (1),
- just bother about file entries (2),
- for each file prepare for awaiting @scala[`Future`]@java[`CompletionStage`] results ignoring the stream order (3),
- run a new stream copying the file contents to a local file (4),
- combine the filename and the copying result (5),
- collect all filenames with results into a sequence (6)

Scala
: @@snip [snip](/doc-examples/src/main/scala/ftpsamples/FtpToFile.scala) { #sample }

Java
: @@snip [snip](/doc-examples/src/main/java/ftpsamples/FtpToFileExample.java) { #sample }

### Example: Rotate data stream over to multiple compressed files on SFTP server

- generate data stream with changing contents over time (1),
- function that tracks last element and outputs a new path when contents in the stream change (2),
- prepare SFTP credentials and settings (3),
- compress ByteStrings (4)

Scala
: @@snip [snip](/doc-examples/src/main/scala/ftpsamples/RotateLogsToFtp.scala) { #sample }

### Running the example code

This example is contained in a stand-alone runnable main, it can be run
 from `sbt` like this:
 

Scala
:   ```
    sbt
    > doc-examples/runMain ftpsamples.FtpToFile
    > doc-examples/runMain ftpsamples.RotateLogsToFtp
    ```

Java
:   ```
    sbt
    > doc-examples/runMain ftpsamples.FtpToFileExample
    ```
