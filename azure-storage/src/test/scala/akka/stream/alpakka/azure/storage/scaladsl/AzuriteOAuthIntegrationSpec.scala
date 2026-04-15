/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka
package azure
package storage
package scaladsl

import akka.actor.ActorSystem
import akka.http.scaladsl.{ConnectionContext, Http}
import akka.stream.Attributes
import com.azure.core.credential.{AccessToken, TokenCredential, TokenRequestContext}
import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer}
import reactor.core.publisher.Mono

import org.testcontainers.utility.MountableFile

import java.io.{File, FileInputStream}
import java.nio.file.Files
import java.security.KeyStore
import java.time.OffsetDateTime
import java.util.Base64
import javax.net.ssl.{SSLContext, TrustManagerFactory}

/**
 * Runs the blob integration tests against Azurite with OAuth enabled (`--oauth basic`),
 * exercising the BearerToken / azure-identity authorization path.
 *
 * Azurite requires HTTPS for Bearer token auth, so we generate a self-signed cert
 * and configure both Azurite and Akka HTTP to use it.
 */
class AzuriteOAuthIntegrationSpec extends StorageIntegrationSpec with ForAllTestContainer {

  private lazy val certDir: File = {
    val dir = Files.createTempDirectory("azurite-certs").toFile
    dir.deleteOnExit()

    val keystore = new File(dir, "keystore.p12")
    val certFile = new File(dir, "cert.pem")
    val keyFile = new File(dir, "key.pem")

    import scala.sys.process._
    Seq(
      "keytool",
      "-genkeypair",
      "-alias",
      "azurite",
      "-keyalg",
      "RSA",
      "-keysize",
      "2048",
      "-storetype",
      "PKCS12",
      "-keystore",
      keystore.getAbsolutePath,
      "-storepass",
      "password",
      "-validity",
      "1",
      "-dname",
      "CN=localhost",
      "-ext",
      "san=dns:localhost,ip:127.0.0.1"
    ).!!

    Seq(
      "openssl",
      "pkcs12",
      "-in",
      keystore.getAbsolutePath,
      "-out",
      certFile.getAbsolutePath,
      "-clcerts",
      "-nokeys",
      "-passin",
      "pass:password"
    ).!!
    Seq(
      "openssl",
      "pkcs12",
      "-in",
      keystore.getAbsolutePath,
      "-out",
      keyFile.getAbsolutePath,
      "-nocerts",
      "-nodes",
      "-passin",
      "pass:password"
    ).!!

    certFile.deleteOnExit()
    keyFile.deleteOnExit()
    keystore.deleteOnExit()

    dir
  }

  override lazy val container: GenericContainer = {
    val c = new GenericContainer(
      dockerImage = "mcr.microsoft.com/azure-storage/azurite:latest",
      exposedPorts = Seq(10000, 10001, 10002),
      command = Seq(
        "azurite",
        "--blobHost",
        "0.0.0.0",
        "--queueHost",
        "0.0.0.0",
        "--tableHost",
        "0.0.0.0",
        "--oauth",
        "basic",
        "--cert",
        "/certs/cert.pem",
        "--key",
        "/certs/key.pem"
      )
    )
    c.container.withCopyFileToContainer(MountableFile.forHostPath(new File(certDir, "cert.pem").getAbsolutePath),
                                        "/certs/cert.pem")
    c.container.withCopyFileToContainer(MountableFile.forHostPath(new File(certDir, "key.pem").getAbsolutePath),
                                        "/certs/key.pem")
    c
  }

  private def blobHostAddress: String =
    s"https://${container.container.getHost}:${container.container.getMappedPort(10000)}"

  override protected implicit val system: ActorSystem = ActorSystem("AzuriteOAuthIntegrationSpec")

  // Set up Akka HTTP to trust the self-signed cert after the container starts
  override def afterStart(): Unit = {
    super.afterStart()
    val keystoreFile = new File(certDir, "keystore.p12")
    val ks = KeyStore.getInstance("PKCS12")
    val fis = new FileInputStream(keystoreFile)
    try ks.load(fis, "password".toCharArray)
    finally fis.close()

    val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    tmf.init(ks)

    val sslContext = SSLContext.getInstance("TLS")
    sslContext.init(null, tmf.getTrustManagers, null)

    Http(system).setDefaultClientHttpsContext(ConnectionContext.httpsClient(sslContext))
  }

  private def fakeJwt: String = {
    val encoder = Base64.getUrlEncoder.withoutPadding()
    val header = encoder.encodeToString("""{"alg":"none","typ":"JWT"}""".getBytes)
    val now = System.currentTimeMillis() / 1000
    val payload = encoder.encodeToString(
      s"""{"aud":"https://storage.azure.com","iss":"https://sts.windows.net/fake/","iat":$now,"nbf":$now,"exp":${now + 3600}}""".getBytes
    )
    s"$header.$payload."
  }

  private val fakeTokenCredential: TokenCredential = new TokenCredential {
    override def getToken(request: TokenRequestContext): Mono[AccessToken] =
      Mono.just(new AccessToken(fakeJwt, OffsetDateTime.now().plusHours(1)))
  }

  protected lazy val blobSettings: StorageSettings =
    StorageExt(system)
      .settings("azurite")
      .withEndPointUrl(blobHostAddress)
      .withTokenCredential(fakeTokenCredential)

  override protected def getDefaultAttributes: Attributes = StorageAttributes.settings(blobSettings)
}
