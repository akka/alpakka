# FTP

### Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-file_$scalaBinaryVersion$
  version=$version$
  group2=com.lightbend.akka
  artifact2=akka-stream-alpakka-ftp_$scalaBinaryVersion$
  version2=$version$
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

### Running the example code

This example is contained in a stand-alone runnable main, it can be run
 from `sbt` like this:
 

Scala
:   ```
    sbt
    > doc-examples/runMain ftpsamples.FtpToFile
    ```

Java
:   ```
    sbt
    > doc-examples/runMain ftpsamples.FtpToFileExample
    ```
