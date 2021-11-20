/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.http

import akka.annotation.InternalApi
import akka.http.scaladsl.HttpsConnectionContext

import java.io.FileInputStream
import java.security.KeyStore
import java.security.cert.{CertificateFactory, X509Certificate}
import javax.net.ssl.{SSLContext, TrustManagerFactory}

@InternalApi
private[google] object ForwardProxyHttpsContext {

  def apply(trustPemPath: String): HttpsConnectionContext = {
    val certificate = x509Certificate(trustPemPath: String)
    val sslContext = SSLContext.getInstance("SSL")

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

  private def x509Certificate(trustPemPath: String): X509Certificate = {
    val stream = new FileInputStream(trustPemPath)
    try CertificateFactory.getInstance("X509").generateCertificate(stream).asInstanceOf[X509Certificate]
    finally stream.close()
  }
}
