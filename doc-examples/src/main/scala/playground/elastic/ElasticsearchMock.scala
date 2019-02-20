/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package playground.elastic

import akka.stream.Materializer
import akka.stream.alpakka.slick.javadsl.SlickSession
import akka.stream.alpakka.slick.scaladsl.Slick
import akka.stream.scaladsl.{Sink, Source}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

object ElasticsearchMock {

  def populateDataForTable()(implicit session: SlickSession, materializer: Materializer) = {

    import session.profile.api._

    //Drop table if already exists
    val dropTableFut =
      sqlu"""drop table if exists MOVIE"""

    //Create movie table
    val createTableFut =
      sqlu"""create table MOVIE (ID INT PRIMARY KEY, TITLE varchar, GENRE varchar, GROSS numeric(10,2))"""

    Await.result(session.db.run(dropTableFut), 10 seconds)
    Await.result(session.db.run(createTableFut), 10 seconds)

    //A class just for organizing the data before using it in the insert clause.  Could have been insertFut with a Tuple too
    case class MovieInsert(id: Int, title: String, genre: String, gross: Double)

    val movies = List(
      MovieInsert(1, "Rogue One", "Adventure", 3.032),
      MovieInsert(2, "Beauty and the Beast", "Musical", 2.795),
      MovieInsert(3, "Wonder Woman", "Action", 2.744),
      MovieInsert(4, "Guardians of the Galaxy", "Action", 2.568),
      MovieInsert(5, "Moana", "Musical", 2.493),
      MovieInsert(6, "Spider-Man", "Action", 1.784)
    )

    Source(movies)
      .via(
        Slick.flow(
          movie => sqlu"INSERT INTO MOVIE VALUES (${movie.id}, ${movie.title}, ${movie.genre}, ${movie.gross})"
        )
      )
      .runWith(Sink.foreach(println))
  }
}
