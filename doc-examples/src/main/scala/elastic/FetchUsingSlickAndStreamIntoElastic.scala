/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package elastic

// #sample
import akka.Done
import akka.stream.alpakka.elasticsearch.WriteMessage._
import akka.stream.alpakka.elasticsearch.scaladsl.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import akka.stream.alpakka.slick.javadsl.SlickSession
import akka.stream.alpakka.slick.scaladsl.Slick
import spray.json.DefaultJsonProtocol._
import spray.json.JsonFormat

import scala.concurrent.Future
import scala.concurrent.duration._
// #sample

import playground.elastic.ElasticsearchMock
import playground.{ActorSystemAvailable, ElasticSearchEmbedded}

object FetchUsingSlickAndStreamIntoElastic extends ActorSystemAvailable with App {

  val runner = ElasticSearchEmbedded.startElasticInstance()
  // format: off
  // #sample

  implicit val session = SlickSession.forConfig("slick-h2-mem")                         // (1)
  actorSystem.registerOnTermination(session.close())

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

  implicit val elasticSearchClient: RestClient =
    RestClient.builder(new HttpHost("localhost", 9201)).build()                         // (4)
  implicit val format: JsonFormat[Movie] = jsonFormat4(Movie)                           // (5)

  val done: Future[Done] =
    Slick
      .source(TableQuery[Movies].result)                                                // (6)
      .map {                                                                            // (7)
        case (id, genre, title, gross) => Movie(id, genre, title, gross)
      }
      .map(movie => createIndexMessage(movie.id.toString, movie))                       // (8)
      .runWith(ElasticsearchSink.create[Movie]("movie", "_doc"))                        // (9)

  done.onComplete {
    case _ =>
      elasticSearchClient.close()                                                       // (10)
  }
  // #sample
  // format: on
  done.onComplete {
    case _ =>
      runner.close()
      runner.clean()
  }
  wait(10.seconds)
  terminateActorSystem()
}
