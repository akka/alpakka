/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp
package impl

import java.io.{File, IOException, InputStream, OutputStream}
import java.nio.file.attribute.PosixFilePermission

import akka.annotation.InternalApi
import net.schmizz.sshj.SSHClient
import net.schmizz.sshj.sftp.{OpenMode, RemoteResourceInfo, SFTPClient}
import net.schmizz.sshj.transport.verification.PromiscuousVerifier
import net.schmizz.sshj.userauth.keyprovider.OpenSSHKeyFile
import net.schmizz.sshj.userauth.password.PasswordUtils
import net.schmizz.sshj.xfer.FilePermission

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.util.Try

/**
 * INTERNAL API
 */
@InternalApi
private[ftp] trait SftpOperations { _: FtpLike[SSHClient, SftpSettings] =>

  type Handler = SFTPClient

  def connect(connectionSettings: SftpSettings)(implicit ssh: SSHClient): Try[Handler] = Try {
    import connectionSettings._

    if (!strictHostKeyChecking)
      ssh.addHostKeyVerifier(new PromiscuousVerifier)
    else
      knownHosts.foreach(path => ssh.loadKnownHosts(new File(path)))

    ssh.connect(host.getHostAddress, port)

    if (credentials.password != "" && sftpIdentity.isEmpty)
      ssh.authPassword(credentials.username, credentials.password)

    sftpIdentity.foreach(setIdentity(_, credentials.username))

    ssh.newSFTPClient()
  }

  def disconnect(handler: Handler)(implicit ssh: SSHClient): Unit = {
    handler.close()
    if (ssh.isConnected) ssh.disconnect()
  }

  def listFiles(basePath: String, handler: Handler): immutable.Seq[FtpFile] = {
    val path = if (!basePath.isEmpty && basePath.head != '/') s"/$basePath" else basePath
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

  def retrieveFileInputStream(name: String, handler: Handler): Try[InputStream] = Try {
    val remoteFile = handler.open(name, Set(OpenMode.READ).asJava)
    val is = new remoteFile.RemoteFileInputStream() {

      override def close(): Unit =
        try {
          super.close()
        } finally {
          remoteFile.close()
        }
    }
    Option(is).getOrElse {
      remoteFile.close()
      throw new IOException(s"$name: No such file or directory")
    }
  }

  def storeFileOutputStream(name: String, handler: Handler, append: Boolean): Try[OutputStream] = Try {
    import OpenMode._
    val openModes = Set(WRITE, CREAT) ++ (if (append) Set(APPEND) else Set(TRUNC))
    val remoteFile = handler.open(name, openModes.asJava)
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

  private[this] def setIdentity(identity: SftpIdentity, username: String)(implicit ssh: SSHClient) = {
    def bats(array: Array[Byte]): String = new String(array, "UTF-8")

    def initKey(f: OpenSSHKeyFile => Unit) = {
      val key = new OpenSSHKeyFile
      f(key)
      ssh.authPublickey(username, key)
    }

    val passphrase =
      identity.privateKeyFilePassphrase.map(pass => PasswordUtils.createOneOff(bats(pass).toCharArray)).orNull

    identity match {
      case id: RawKeySftpIdentity =>
        initKey(_.init(bats(id.privateKey), id.publicKey.map(bats).orNull, passphrase))
      case id: KeyFileSftpIdentity =>
        initKey(_.init(new File(id.privateKey), passphrase))
    }
  }

  def move(fromPath: String, destinationPath: String, handler: Handler): Unit =
    handler.rename(fromPath, destinationPath)

  def remove(path: String, handler: Handler): Unit =
    handler.rm(path)
}
