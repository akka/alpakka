package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import java.io.FileInputStream
import java.security.KeyStore
import java.security.cert.{CertificateFactory, X509Certificate}

import akka.actor.ActorSystem
import akka.http.scaladsl.HttpsConnectionContext
import akka.stream.alpakka.googlecloud.bigquery.ForwardProxy
import javax.net.ssl.{SSLContext, TrustManagerFactory}
;

private[bigquery] object ForwardProxyHttpsContext {

  implicit class ForwardProxyPoolSettings(forwardProxy: ForwardProxy) {

    def httpsContext(system: ActorSystem) = {
      val certificate = x509Certificate(forwardProxy)
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
  }

  private def x509Certificate(forwardProxy: ForwardProxy) = {
    val stream = new FileInputStream(forwardProxy.getForwardProxyTrustPem.get().pemPath)
    var result: X509Certificate = null
    try result = CertificateFactory.getInstance("X509").generateCertificate(stream).asInstanceOf[X509Certificate]
    finally if (stream!= null) stream.close()
    result
  }

}
