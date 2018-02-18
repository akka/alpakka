/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package elastic

// format: off
// #sample
import akka.stream.alpakka.elasticsearch.IncomingMessage
import akka.stream.alpakka.elasticsearch.scaladsl.ElasticsearchSink
import akka.stream.alpakka.slick.javadsl.SlickSession
import akka.stream.alpakka.slick.scaladsl.Slick
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import playground.elastic.ElasticsearchMock
import playground.{ActorSystemAvailable, ElasticSearchEmbedded}
import spray.json.DefaultJsonProtocol._
import spray.json.JsonFormat

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
// #sample
// format: off

object FetchUsingSlickAndStreamIntoElastic extends ActorSystemAvailable with App {

  val runner = ElasticSearchEmbedded.startElasticInstance()
  // format: off
  // #sample


  implicit val session = SlickSession.forConfig("slick-h2-mem")                         // (1)

  import session.profile.api._
  // #sample
  // format: on
  ElasticsearchMock.populateDataForTable()

  // format: off
  // #sample
  class Movies(tag: Tag) extends Table[(Int, String, String, Double)](tag, "MOVIE") {   // (2)
    def id = column[Int]("ID")
    def title = column[String]("TITLE")
    def genre = column[String]("GENRE")
    def gross = column[Double]("GROSS")

    override def * = (id, title, genre, gross)
  }

  case class Movie(id: Int, title: String, genre: String, gross: Double)                // (3)

  val fetchDataFromTableFlow: Source[Movie, NotUsed] =                                  // (4)
    Slick
      .source(TableQuery[Movies].result)                                                // (5)
      .map {                                                                            // (6)
      case (id, genre, title, gross) =>
        Movie(id, genre, title, gross)
    }


  implicit val client = RestClient.builder(new HttpHost("localhost", 9201)).build()     // (7)
  implicit val format: JsonFormat[Movie] = jsonFormat4(Movie)                           // (8)

  val elasticSearchSink = ElasticsearchSink.create[Movie]("movie", "boxoffice")         // (9)

  val done: Future[Done] = fetchDataFromTableFlow
    .map(movie => IncomingMessage(Option(movie.id).map(_.toString), movie))             // (10)
    .runWith(elasticSearchSink)

  done.onComplete {
    case _ =>
      session.close()
      client.close()                                                                    // (11)
      runner.close()
      runner.clean()
  }

  // #sample
  // format: on
  wait(10 seconds)
  terminateActorSystem()
}
