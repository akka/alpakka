/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.net.InetAddress

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.stream.alpakka.ftp.scaladsl.{Sftp, SftpApi}
import akka.stream.alpakka.ftp.{FtpCredentials, SftpSettings}
import akka.stream.scaladsl.Sink
import net.schmizz.sshj.transport.verification.PromiscuousVerifier
import net.schmizz.sshj.userauth.method.AuthPassword
import net.schmizz.sshj.userauth.password.{PasswordFinder, Resource}
import net.schmizz.sshj.{DefaultConfig, SSHClient}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class StreamingSftpTransport {
  implicit val system: ActorSystem = ActorSystem("my-service")

  private val PORT = 22
  private val USER = "conor.griffin"
  private val CREDENTIALS = FtpCredentials.create(USER, "password!")
  private val BASE_PATH = s"/Users/$USER"
  private val FILE_NAME = "10mfile"
  private val CHUNK_SIZE = 131072

  // Set up the source system connection
  private val SOURCE_HOSTNAME = "suv1"

  private val sourceSettings = SftpSettings(host = InetAddress.getByName(SOURCE_HOSTNAME))
    .withCredentials(FtpCredentials.create("testsftp", "t3st123"))
    .withPort(PORT)
    .withStrictHostKeyChecking(false)

  private val sourceClient = new SSHClient(new DefaultConfig) {}
  private val configuredSourceClient: SftpApi = Sftp(sourceClient)

  // Set up the destination system connection

  private val DEST_HOSTNAME = "localhost"
  private val destSettings = SftpSettings(host = InetAddress.getByName(DEST_HOSTNAME))
    .withCredentials(CREDENTIALS)
    .withPort(PORT)
    .withStrictHostKeyChecking(false)

  private val destClient = new SSHClient(new DefaultConfig)
  private val configuredDestClient: SftpApi = Sftp(destClient)

  private val decider: Supervision.Decider = {
    case a =>
      print(a.getMessage)
      Supervision.resume
  }

  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))

  def doTransfer(): Unit = {
    println("Streaming")
    val source = configuredSourceClient.fromPath(s"/home/testsftp/$FILE_NAME", sourceSettings, CHUNK_SIZE)
    // val sink = configuredDestClient.toPath(s"$BASE_PATH/$FILE_NAME.out", destSettings)
    val runnable = source
      .runWith(Sink.ignore)

    println("Streaming: Starting")
    val start = System.currentTimeMillis()
    Await.result(runnable, 180.seconds)
    val end = System.currentTimeMillis()
    println(s"Streaming: ${end - start}")

  }

  def doSftpTransfer(): Unit = {
    println("SFTP")
    val ssh = new SSHClient(new DefaultConfig)
    ssh.addHostKeyVerifier(new PromiscuousVerifier)
    ssh.connect(SOURCE_HOSTNAME, 22)
    val passwordAuth: AuthPassword = new AuthPassword(new PasswordFinder() {
      def reqPassword(resource: Resource[_]): Array[Char] = "t3st123".toCharArray
      def shouldRetry(resource: Resource[_]) = false
    })
    ssh.auth("testsftp", passwordAuth)

    println("SFTP: Starting")
    val start = System.currentTimeMillis()
    ssh.newSFTPClient().get("/home/testsftp/10mfile", "/Users/conor.griffin/Downloads/10mfile.sftp")
    val end = System.currentTimeMillis()
    println(s"SFTP: ${end - start}")

  }

}
