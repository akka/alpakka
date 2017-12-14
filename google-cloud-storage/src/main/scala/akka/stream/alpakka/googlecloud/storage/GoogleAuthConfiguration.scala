/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

import java.nio.file.{Files, Path, Paths}
import java.security.{KeyFactory, PrivateKey}
import java.security.spec.PKCS8EncodedKeySpec
import java.util.Base64

import com.typesafe.config.Config
import spray.json._
import spray.json.DefaultJsonProtocol._

import scala.io.Source

object GoogleAuthConfiguration {

  private final case class ServiceAccountFile(project_id: String, private_key: String, client_email: String)

  private implicit val serviceAccountFileFormat = jsonFormat3(ServiceAccountFile)

  private val configPath = "alpakka.google.serviceAccountFile"

  private def readServiceAccountFile(serviceAccountFile: Path): ServiceAccountFile = {
    if (Files.notExists(serviceAccountFile)) {
      throw new RuntimeException(s"Service account file missing: ${serviceAccountFile.toAbsolutePath}")
    }
    val bufferedSource = Source.fromFile(serviceAccountFile.toFile)
    val contentAsJson = bufferedSource.getLines().mkString.parseJson
    bufferedSource.close()
    contentAsJson.convertTo[ServiceAccountFile]
  }

  private def parsePrivateKey(privateKey: String): PrivateKey = {
    val pk = privateKey
      .replace("-----BEGIN RSA PRIVATE KEY-----\n", "")
      .replace("-----END RSA PRIVATE KEY-----", "")
      .replace("-----BEGIN PRIVATE KEY-----\n", "")
      .replace("-----END PRIVATE KEY-----", "")
      .replaceAll(raw"\s", "")
    val kf = KeyFactory.getInstance("RSA")
    val encodedPv = Base64.getDecoder.decode(pk)
    val keySpecPv = new PKCS8EncodedKeySpec(encodedPv)
    kf.generatePrivate(keySpecPv)
  }

  def apply(serviceAccountFile: Path): GoogleAuthConfiguration = {
    val serviceAccount = readServiceAccountFile(serviceAccountFile)
    val privateKey = parsePrivateKey(serviceAccount.private_key)
    GoogleAuthConfiguration(privateKey, serviceAccount.client_email, serviceAccount.project_id)
  }

  def apply(config: Config): Option[GoogleAuthConfiguration] =
    Some(config)
      .filter(_.hasPath(configPath))
      .map(_.getString(configPath))
      .filter(_.trim.nonEmpty)
      .map(file => apply(Paths.get(file)))

}

final case class GoogleAuthConfiguration(privateKey: PrivateKey, clientEmail: String, projectId: String)
