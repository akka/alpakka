/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package ftpsamples

// #sample
import java.net.InetAddress
import java.nio.file.Paths

import akka.stream.IOResult
import akka.stream.alpakka.ftp.FtpSettings
import akka.stream.alpakka.ftp.scaladsl.Ftp
import akka.stream.scaladsl.{FileIO, Sink}
import org.apache.mina.util.AvailablePortFinder
import playground.filesystem.FileSystemMock
import playground.{ActorSystemAvailable, FtpServerEmbedded}

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.{Failure, Success}

// #sample

object FtpToFile extends ActorSystemAvailable with App {

  val ftpFileSystem = new FileSystemMock()

  val port = AvailablePortFinder.getNextAvailable(21000)
  val ftpServer = FtpServerEmbedded.start(ftpFileSystem.fileSystem, port)

  ftpFileSystem.generateFiles(30, 10, "/home/anonymous")
  ftpFileSystem.putFileOnFtp("/home/anonymous", "hello.txt")
  ftpFileSystem.putFileOnFtp("/home/anonymous", "hello2.txt")

  // #sample
  val ftpSettings = FtpSettings(InetAddress.getByName("localhost")).withPort(port)

  // #sample

  val targetDir = Paths.get("target/")
  val fetchedFiles: Future[immutable.Seq[(String, IOResult)]] =
    // format: off
    // #sample
    Ftp
      .ls("/", ftpSettings)                                    //: FtpFile (1)
      .filter(ftpFile => ftpFile.isFile)                       //: FtpFile (2)
      .mapAsyncUnordered(parallelism = 5) { ftpFile =>         // (3)
        val localPath = targetDir.resolve("." + ftpFile.path)
        val fetchFile: Future[IOResult] = Ftp
          .fromPath(ftpFile.path, ftpSettings)                
          .runWith(FileIO.toPath(localPath))                   // (4)
        fetchFile.map { ioResult =>                            // (5)
          (ftpFile.path, ioResult)
        }
      }                                                        //: (String, IOResult)
      .runWith(Sink.seq)                                       // (6)
    // #sample
  // format: on
  fetchedFiles
    .map { files =>
      files.filter { case (_, r) => !r.wasSuccessful }
    }
    .onComplete { res =>
      res match {
        case Success(errors) if errors.isEmpty =>
          println("all files fetched.")
        case Success(errors) =>
          println(s"errors occured: ${errors.mkString("\n")}")
        case Failure(exception) =>
          println("the stream failed")
      }
      actorSystem.terminate().onComplete { _ =>
        ftpServer.stop()
      }
    }
}
