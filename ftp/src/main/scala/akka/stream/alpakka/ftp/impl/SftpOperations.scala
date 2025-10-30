/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.ftp
package impl

import java.io.{File, IOException, InputStream, OutputStream}
import java.nio.file.attribute.PosixFilePermission
import java.nio.charset.StandardCharsets

import akka.annotation.InternalApi
import net.schmizz.sshj.SSHClient
import net.schmizz.sshj.sftp.{OpenMode, RemoteResourceInfo, SFTPClient}
import net.schmizz.sshj.transport.verification.PromiscuousVerifier
import net.schmizz.sshj.userauth.UserAuthException
import net.schmizz.sshj.userauth.method.{AuthPassword, AuthPublickey}
import net.schmizz.sshj.userauth.password.{PasswordFinder, PasswordUtils, Resource}
import net.schmizz.sshj.xfer.FilePermission
import org.apache.commons.net.DefaultSocketFactory

import scala.jdk.CollectionConverters._
import scala.collection.immutable
import scala.util.{Failure, Try}

/**
 * INTERNAL API
 */
@InternalApi
private[ftp] trait SftpOperations { ftpLike: FtpLike[SSHClient, SftpSettings] =>

  type Handler = SFTPClient

  def connect(connectionSettings: SftpSettings)(implicit ssh: SSHClient): Try[Handler] =
    Try {
      import connectionSettings._

      proxy.foreach(p => ssh.setSocketFactory(new DefaultSocketFactory(p)))

      if (!strictHostKeyChecking) {
        ssh.addHostKeyVerifier(new PromiscuousVerifier)
      } else {
        knownHosts.foreach(path => ssh.loadKnownHosts(new File(path)))
      }
      ssh.connect(host.getHostAddress, port)

      sftpIdentity match {
        case Some(identity) =>
          val keyAuth = authPublickey(identity)

          if (credentials.password != "") {
            val passwordAuth: AuthPassword = new AuthPassword(new PasswordFinder() {
              def reqPassword(resource: Resource[_]): Array[Char] = credentials.password.toCharArray

              def shouldRetry(resource: Resource[_]) = false
            })

            ssh.auth(credentials.username, passwordAuth, keyAuth)
          } else {
            ssh.auth(credentials.username, keyAuth)
          }
        case None =>
          if (credentials.password != "") {
            ssh.authPassword(credentials.username, credentials.password)
          }
      }

      ssh.newSFTPClient()
    } match {
      case Failure(_: UserAuthException) =>
        throw new FtpAuthenticationException(
          s"unable to login to host=[${connectionSettings.host}], port=${connectionSettings.port} ${connectionSettings.proxy
            .fold("")("proxy=" + _.toString)}"
        )
      case result => result
    }

  def disconnect(handler: Handler)(implicit ssh: SSHClient): Unit = {
    handler.close()
    if (ssh.isConnected) ssh.disconnect()
  }

  def listFiles(basePath: String, handler: Handler): immutable.Seq[FtpFile] = {
    val path = if (basePath.nonEmpty && basePath.head != '/') s"/$basePath" else basePath
    val entries = handler.ls(path).asScala
    entries.map { file =>
      FtpFile(
        file.getName,
        file.getPath,
        file.isDirectory,
        file.getAttributes.getSize,
        file.getAttributes.getMtime * 1000L,
        getPosixFilePermissions(file)
      )
    }.toVector
  }

  def mkdir(path: String, name: String, handler: Handler): Unit = {
    val updatedPath = CommonFtpOperations.concatPath(path, name)
    handler.mkdirs(updatedPath)
  }

  private def getPosixFilePermissions(file: RemoteResourceInfo) = {
    import PosixFilePermission._

    import FilePermission._
    file.getAttributes.getPermissions.asScala.collect {
      case USR_R => OWNER_READ
      case USR_W => OWNER_WRITE
      case USR_X => OWNER_EXECUTE
      case GRP_R => GROUP_READ
      case GRP_W => GROUP_WRITE
      case GRP_X => GROUP_EXECUTE
      case OTH_R => OTHERS_READ
      case OTH_W => OTHERS_WRITE
      case OTH_X => OTHERS_EXECUTE
    }.toSet
  }

  def listFiles(handler: Handler): immutable.Seq[FtpFile] = listFiles(".", handler)

  def retrieveFileInputStream(name: String, handler: Handler): Try[InputStream] =
    retrieveFileInputStream(name, handler, 0L)

  def retrieveFileInputStream(name: String, handler: Handler, offset: Long): Try[InputStream] =
    retrieveFileInputStream(name, handler, offset, 1)

  def retrieveFileInputStream(name: String,
                              handler: Handler,
                              offset: Long,
                              maxUnconfirmedReads: Int): Try[InputStream] =
    Try {
      val remoteFile = handler.open(name, java.util.EnumSet.of(OpenMode.READ))
      val is = maxUnconfirmedReads match {
        case m if m > 1 =>
          new remoteFile.ReadAheadRemoteFileInputStream(m, offset, 2048L) {

            override def close(): Unit =
              try {
                super.close()
              } finally {
                remoteFile.close()
              }
          }
        case _ =>
          new remoteFile.RemoteFileInputStream(offset) {

            override def close(): Unit =
              try {
                super.close()
              } finally {
                remoteFile.close()
              }
          }
      }
      Option(is).getOrElse {
        remoteFile.close()
        throw new IOException(s"$name: No such file or directory")
      }
    }

  def storeFileOutputStream(name: String, handler: Handler, append: Boolean): Try[OutputStream] =
    Try {
      import OpenMode._
      val openModes =
        if (append) java.util.EnumSet.of(WRITE, CREAT, APPEND)
        else java.util.EnumSet.of(WRITE, CREAT, TRUNC)
      val remoteFile = handler.open(name, openModes)
      val os = new remoteFile.RemoteFileOutputStream() {

        override def close(): Unit = {
          try {
            remoteFile.close()
          } catch {
            case e: IOException =>
          }
          super.close()
        }
      }
      Option(os).getOrElse {
        remoteFile.close()
        throw new IOException(s"Could not write to $name")
      }
    }

  private[this] def authPublickey(identity: SftpIdentity)(implicit ssh: SSHClient) = {
    def bats(array: Array[Byte]): String = new String(array, StandardCharsets.UTF_8)

    val passphrase =
      identity.privateKeyFilePassphrase
        .map(pass => PasswordUtils.createOneOff(bats(pass).toCharArray))
        .orNull

    identity match {
      case id: RawKeySftpIdentity =>
        new AuthPublickey(
          ssh.loadKeys(bats(id.privateKey), id.publicKey.map(bats).orNull, passphrase)
        )
      case id: KeyFileSftpIdentity =>
        new AuthPublickey(ssh.loadKeys(id.privateKey, passphrase))
    }
  }

  def move(fromPath: String, destinationPath: String, handler: Handler): Unit =
    handler.rename(fromPath, destinationPath)

  def remove(path: String, handler: Handler): Unit =
    handler.rm(path)
}
