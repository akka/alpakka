/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp

object scalaExamples {

  // settings
  object settings {
    //#create-settings
    import akka.stream.alpakka.ftp.FtpCredentials.AnonFtpCredentials
    import java.net.InetAddress

    val settings = FtpSettings(
      InetAddress.getByName("localhost"),
      credentials = AnonFtpCredentials,
      binary = true,
      passiveMode = true
    )
    //#create-settings
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
}
