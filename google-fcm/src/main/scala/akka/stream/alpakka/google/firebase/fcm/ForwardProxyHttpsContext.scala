/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm

import java.io.FileInputStream
import java.security.KeyStore
import java.security.cert.{CertificateFactory, X509Certificate}

import akka.actor.ActorSystem
import akka.http.scaladsl.{Http, HttpsConnectionContext}
import javax.net.ssl.{SSLContext, TrustManagerFactory}

private[fcm] object ForwardProxyHttpsContext {

  val SSL = "SSL"
  val X509 = "X509"

  implicit class ForwardProxyHttpsContext(forwardProxy: ForwardProxy) {

    def httpsContext(system: ActorSystem) = {
      forwardProxy.trustPem match {
        case Some(trustPem) => createHttpsContext(trustPem)
        case None => defaultHttpsContext(system)
      }
    }
  }

  private def defaultHttpsContext(implicit system: ActorSystem) = {
    Http().createDefaultClientHttpsContext()
  }

  private def createHttpsContext(trustPem: ForwardProxyTrustPem) = {
    val certificate = x509Certificate(trustPem)
    val sslContext = SSLContext.getInstance(SSL)

    val alias = certificate.getIssuerDN.getName
    val trustStore = KeyStore.getInstance(KeyStore.getDefaultType)
    trustStore.load(null, null)
    trustStore.setCertificateEntry(alias, certificate)

    val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    tmf.init(trustStore)
    val trustManagers = tmf.getTrustManagers
    sslContext.init(null, trustManagers, null)
    new HttpsConnectionContext(sslContext)
  }

  private def x509Certificate(trustPem: ForwardProxyTrustPem) = {
    val stream = new FileInputStream(trustPem.pemPath)
    var result: X509Certificate = null
    try result = CertificateFactory.getInstance(X509).generateCertificate(stream).asInstanceOf[X509Certificate]
    finally if (stream != null) stream.close()
    result
  }

}
