/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp

object scalaExamples {

  // settings
  object settings {
    //#create-settings
    import akka.stream.alpakka.ftp.FtpSettings
    import akka.stream.alpakka.ftp.FtpCredentials.AnonFtpCredentials
    import org.apache.commons.net.PrintCommandListener
    import org.apache.commons.net.ftp.FTPClient
    import java.io.PrintWriter
    import java.net.InetAddress

    val settings = FtpSettings(
      InetAddress.getByName("localhost"),
      credentials = AnonFtpCredentials,
      binary = true,
      passiveMode = true,
      configureConnection = (ftpClient: FTPClient) => {
        ftpClient.addProtocolCommandListener(new PrintCommandListener(new PrintWriter(System.out), true))
      }
    )
    //#create-settings
  }

  object sshConfigure {
    //#configure-custom-ssh-client
    import akka.stream.alpakka.ftp.scaladsl.{Sftp, SftpApi}
    import net.schmizz.sshj.DefaultConfig
    import net.schmizz.sshj.SSHClient

    val sshClient: SSHClient = new SSHClient(new DefaultConfig)
    val configuredClient: SftpApi = Sftp(sshClient)
    //#configure-custom-ssh-client
  }

  object traversing {
    //#traversing
    import akka.NotUsed
    import akka.stream.alpakka.ftp.scaladsl.Ftp
    import akka.stream.scaladsl.Source

    def listFiles(basePath: String, settings: FtpSettings): Source[FtpFile, NotUsed] =
      Ftp.ls(basePath, settings)

    //#traversing
  }

  object retrieving {
    //#retrieving
    import akka.stream.IOResult
    import akka.stream.alpakka.ftp.scaladsl.Ftp
    import akka.stream.scaladsl.Source
    import akka.util.ByteString
    import scala.concurrent.Future

    def retrieveFromPath(path: String, settings: FtpSettings): Source[ByteString, Future[IOResult]] =
      Ftp.fromPath(path, settings)

    //#retrieving
  }

  object storing {
    //#storing
    import akka.stream.IOResult
    import akka.stream.alpakka.ftp.scaladsl.Ftp
    import akka.stream.scaladsl.Sink
    import akka.util.ByteString
    import scala.concurrent.Future

    def storeToPath(path: String, settings: FtpSettings, append: Boolean): Sink[ByteString, Future[IOResult]] =
      Ftp.toPath(path, settings, append)
    //#storing
  }

  object removing {
    //#removing
    import akka.stream.IOResult
    import akka.stream.alpakka.ftp.scaladsl.Ftp
    import akka.stream.scaladsl.Sink
    import scala.concurrent.Future

    def remove(settings: FtpSettings): Sink[FtpFile, Future[IOResult]] =
      Ftp.remove(settings)
    //#removing
  }

  object move {
    //#moving
    import akka.stream.IOResult
    import akka.stream.alpakka.ftp.scaladsl.Ftp
    import akka.stream.scaladsl.Sink
    import scala.concurrent.Future

    def move(destinationPath: FtpFile => String, settings: FtpSettings): Sink[FtpFile, Future[IOResult]] =
      Ftp.move(destinationPath, settings)
    //#moving
  }

  object processAndMove {
    //#processAndMove
    import akka.NotUsed
    import akka.stream.alpakka.ftp.scaladsl.Ftp
    import akka.stream.scaladsl.FileIO
    import akka.stream.scaladsl.RunnableGraph
    import java.nio.file.Files

    def processAndMove(sourcePath: String,
                       destinationPath: FtpFile => String,
                       settings: FtpSettings): RunnableGraph[NotUsed] =
      Ftp
        .ls(sourcePath, settings)
        .flatMapConcat(ftpFile => Ftp.fromPath(ftpFile.path, settings).map((_, ftpFile)))
        .alsoTo(FileIO.toPath(Files.createTempFile("downloaded", "tmp")).contramap(_._1))
        .to(Ftp.move(destinationPath, settings).contramap(_._2))
    //#processAndMove
  }
}
