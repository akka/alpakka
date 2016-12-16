/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.auth

import java.io.File

import akka.actor.ActorSystem
import akka.stream.{ ActorMaterializer, ActorMaterializerSettings, Materializer }
import akka.testkit.TestKit
import org.scalatest.{ FlatSpecLike, Matchers }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{ Millis, Seconds, Span }
import java.nio.file.Files
import java.time.LocalDate

import akka.http.scaladsl.model.HttpHeader.ParsingResult
import akka.http.scaladsl.model.{ Uri, _ }

import scala.io.Source
import scala.util.{ Failure, Success, Try }
import scala.language.implicitConversions

/**
 * Validates implementation of AWS Signature version 4.
 */
class SignerSpec4(_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with ScalaFutures with AWS4Spec {
  def this() = this(ActorSystem("SignerSpec4"))

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(2, Seconds), interval = Span(5, Millis))

  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withDebugLogging(true))

  val credentials = AWSCredentials("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY")
  val scope = CredentialScope(LocalDate.of(2015, 8, 30), "us-east-1", "aws4_request")
  val signingKey = SigningKey(credentials, scope)

  val reqs = Probes.rawRequests
  val creqs = Probes.canonicalRequests

  it should "produce proper canonical requests" in {
    val probes: Try[List[(HttpRequest, (String, String))]] = for {
      req <- reqs
      crq <- creqs
    } yield req.zip(crq)

    probes.toOption shouldBe defined

    probes.get.foreach(pair => testCanonicalRequest(pair._1, pair._2))

  }

}

trait AWS4Spec extends Matchers { this: FlatSpecLike =>

  implicit def rawHttpRequest(source: Source): HttpRequestFromSource = new HttpRequestFromSource(source)

  /**
   * Validate Canonical request builder.
   *
   * @param req  - the web request to be signed.
   * @param creq - the expected canonical request.
   */
  def testCanonicalRequest(req: HttpRequest, creq: (String, String))(implicit mat: Materializer): Unit = {
    val canonicalRequest = CanonicalRequest.from(req).canonicalString
    withClue(s"According to expected canonical request stored in ${creq._1}") {
      canonicalRequest should equal(creq._2)
    }
  }

}

class HttpRequestFromSource(source: Source) {
  private val pattern = """(^\s*\w+\s)(.+)(\sHTTP\/...)""".r

  def fromSource: HttpRequest = {
    val lines = source.getLines().toList
    val pattern(rawMethod, rawUri, rawProtocol) = lines.head.filterNot(_ == '\uFEFF')
    val body = if (lines.reverse.head.startsWith("X-")) "" else lines.reverse.head

    val headers = loadHeaders(lines.tail.takeWhile(!_.isEmpty))
    HttpRequest(HttpMethods.getForKey(rawMethod.trim).getOrElse(HttpMethods.CONNECT), encodeIfRequired(rawUri),
      headers, HttpEntity(body), HttpProtocols.getForKey(rawProtocol).getOrElse(HttpProtocols.`HTTP/1.1`))
  }

  private def encodeIfRequired(rawUri: String): Uri = {
    val pattern = """^([!#$&-;=?-\[\]_a-z~]|%[0-9a-fA-F]{2})+$$""".r
    // TODO: this look like a bug in Uri parser in akka-http ("//" is generating empty path)
    val unparsedUri = rawUri.replace("//", "/")
    val uri = Try {
      val pattern(result) = unparsedUri
      result
    }
    uri match {
      case Success(_) => Uri(unparsedUri)
      // TODO: Missing utf-8 support in Uri class from akka-http
      case Failure(_) =>
        if (unparsedUri.startsWith("/?"))
          Uri("/?" + java.net.URLEncoder.encode(unparsedUri.drop(2).takeWhile(_ != " "), "UTF-8").replace("%3D", "="))
        else
          Uri("/" + java.net.URLEncoder.encode(unparsedUri.drop(1), "UTF-8"))
    }
  }

  private def loadHeaders(headers: Seq[String]): List[HttpHeader] = {

    val lines: List[String] = headers.foldLeft(List.empty[String]) { (acc, current) =>
      if (current.contains(':')) {
        current :: acc
      } else {
        // TODO: trim and ',' has been added to pass the test, but it looks like a bug in HttpParser code from akka-http.
        (acc.head + "," + current.trim) :: acc.tail
      }
    }

    lines.reverse
      .map(_.split(":"))
      .filter(_.length == 2)
      .map(h => (h(0), h(1)))
      .map(rawHeader => HttpHeader.parse(rawHeader._1, rawHeader._2))
      .map {
        case ParsingResult.Error(l) => None
        case ParsingResult.Ok(rawHeaders, _) => Some(rawHeaders)
      }
      .filter(_.isDefined)
      .map(_.get)
  }
}

/**
 * Load the probes files from resource folder.
 */
object Probes {

  import scala.collection.JavaConversions._

  private val probes = Try {
    Files
      .walk(new File(getClass.getResource("/signature4.probes/aws4_testsuite").toURI).toPath)
      .iterator()
      .filter(Files.isRegularFile(_))
      .map(_.toAbsolutePath)
      .toList
  }

  implicit def rawHttpRequest(source: Source): HttpRequestFromSource = new HttpRequestFromSource(source)

  def rawRequests: Try[List[HttpRequest]] = {
    val reqPaths = probes.map(_.filter(_.toString.endsWith(".req")))
    val reqSource = reqPaths.map(ip => ip.map(t => Source.fromFile(t.toFile, "UTF-8")))
    val req = reqSource.map(_.map(_.fromSource))
    reqSource.foreach(_.foreach(_.close()))
    req
  }

  def canonicalRequests: Try[List[(String, String)]] = {
    val creqPaths = probes.map(_.filter(_.toString.endsWith(".creq")))
    val creqSources = creqPaths.map(ip => ip.map(t => (t.getFileName.toString, Source.fromFile(t.toFile, "UTF-8"))))
    val creq = creqSources.map(l => l.map(t => (t._1, t._2.mkString)))
    creqSources.foreach(_.foreach(_._2.close()))
    creq
  }

}
