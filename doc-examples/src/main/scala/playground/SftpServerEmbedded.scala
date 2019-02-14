/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package playground

import java.io.File
import java.nio.charset.Charset
import java.nio.file.{FileSystem, Files, Path, Paths}
import java.security.PublicKey
import java.util

import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory
import org.apache.sshd.common.keyprovider.FileKeyPairProvider
import org.apache.sshd.server.SshServer
import org.apache.sshd.server.auth.password.PasswordAuthenticator
import org.apache.sshd.server.auth.pubkey.PublickeyAuthenticator
import org.apache.sshd.server.scp.ScpCommandFactory
import org.apache.sshd.server.session.ServerSession
import org.apache.sshd.server.subsystem.sftp.SftpSubsystemFactory

object SftpServerEmbedded {

  val FtpRootDir = "/tmp/home"
  val hostname = "localhost"

  val resourcePath = "./doc-examples/src/main/resources"

  val clientPrivateKeyPassphrase: Array[Byte] = "secret".getBytes(Charset.forName("UTF-8"))
  var clientPrivateKeyFile: String = new File(s"$resourcePath/id_rsa").getAbsolutePath
  val keyPairProviderFile: String = new File(s"$resourcePath/hostkey.pem").getAbsolutePath

  private var sshd: SshServer = _

  def start(fileSystem: FileSystem, port: Int): Unit = {
    sshd = SshServer.setUpDefaultServer()
    sshd.setHost(hostname)
    sshd.setPort(port)
    sshd.setKeyPairProvider(new FileKeyPairProvider(Paths.get(keyPairProviderFile)))
    sshd.setSubsystemFactories(util.Arrays.asList(new SftpSubsystemFactory))
    sshd.setCommandFactory(new ScpCommandFactory())
    val passwordAuthenticator = new PasswordAuthenticator() {
      override def authenticate(username: String, password: String, session: ServerSession): Boolean =
        username != null && username == password
    }
    sshd.setPasswordAuthenticator(passwordAuthenticator)
    val publicKeyAuthenticator = new PublickeyAuthenticator() {
      override def authenticate(username: String, key: PublicKey, session: ServerSession): Boolean = true
    }
    sshd.setPublickeyAuthenticator(publicKeyAuthenticator)

    val home: Path = fileSystem.getPath(FtpRootDir)
    sshd.setFileSystemFactory(new VirtualFileSystemFactory(home))
    sshd.start()
    if (!Files.exists(home)) Files.createDirectories(home)
  }

  def stopServer(): Unit =
    try {
      sshd.stop(true)
    } catch {
      case t: Throwable =>
        throw new RuntimeException(t)
    }
}
