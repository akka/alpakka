/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package example

//#important-imports
import akka.stream.scaladsl._
import akka.stream.alpakka.slick.scaladsl._
//#important-imports

//#source-example
import scala.concurrent.Future

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

import akka.stream.scaladsl._
import akka.stream.alpakka.slick.scaladsl._

import slick.jdbc.GetResult

object SlickSourceWithPlainSQLQueryExample extends App {
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  implicit val ec = system.dispatcher

  implicit val session = SlickSession.forConfig("slick-h2")

  // The example domain
  case class User(id: Int, name: String)

  // We need this to automatically transform result rows
  // into instances of the User class.
  // Please import slick.jdbc.GetResult
  // See also: "http://slick.lightbend.com/doc/3.2.1/sql.html#result-sets"
  implicit val getUserResult = GetResult(r => User(r.nextInt, r.nextString))

  // This import enables the use of the Slick sql"...",
  // sqlu"...", and sqlt"..." String interpolators.
  // See also: "http://slick.lightbend.com/doc/3.2.1/sql.html#string-interpolation"
  import session.profile.api._

  // Stream the results of a query
  val done: Future[Done] =
    Slick
      .source(sql"SELECT ID, NAME FROM ALPAKKA_SLICK_SCALADSL_TEST_USERS".as[User])
      .log("user")
      .runWith(Sink.ignore)

  done.onComplete {
    case _ =>
      session.close()
      system.terminate()
  }
}
//#source-example

//#source-with-typed-query
import scala.concurrent.Future

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

import akka.stream.scaladsl._
import akka.stream.alpakka.slick.scaladsl._

object SlickSourceWithTypedQueryExample extends App {
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  implicit val ec = system.dispatcher

  implicit val session = SlickSession.forConfig("slick-h2")

  // This import brings everything you need into scope
  import session.profile.api._

  // The example domain
  class Users(tag: Tag) extends Table[(Int, String)](tag, "ALPAKKA_SLICK_SCALADSL_TEST_USERS") {
    def id = column[Int]("ID")
    def name = column[String]("NAME")
    def * = (id, name)
  }

  // Stream the results of a query
  val done: Future[Done] =
    Slick
      .source(TableQuery[Users].result)
      .log("user")
      .runWith(Sink.ignore)

  done.onComplete {
    case _ =>
      session.close()
      system.terminate()
  }
}
//#source-with-typed-query

//#sink-example
import scala.concurrent.Future

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

import akka.stream.scaladsl._
import akka.stream.alpakka.slick.scaladsl._

object SlickSinkExample extends App {
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  implicit val ec = system.dispatcher

  implicit val session = SlickSession.forConfig("slick-h2")

  // The example domain
  case class User(id: Int, name: String)
  val users = (1 to 42).map(i => User(i, s"Name$i"))

  // This import enables the use of the Slick sql"...",
  // sqlu"...", and sqlt"..." String interpolators.
  // See "http://slick.lightbend.com/doc/3.2.1/sql.html#string-interpolation"
  import session.profile.api._

  // Stream the users into the database as insert statements
  val done: Future[Done] =
    Source(users)
      .runWith(
        // add an optional first argument to specify the parallism factor (Int)
        Slick.sink(user => sqlu"INSERT INTO ALPAKKA_SLICK_SCALADSL_TEST_USERS VALUES(${user.id}, ${user.name})")
      )

  done.onComplete {
    case _ =>
      session.close()
      system.terminate()
  }
}
//#sink-example

//#flow-example
import scala.concurrent.Future

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

import akka.stream.scaladsl._
import akka.stream.alpakka.slick.scaladsl._

object SlickFlowExample extends App {
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  implicit val ec = system.dispatcher

  implicit val session = SlickSession.forConfig("slick-h2")

  // The example domain
  case class User(id: Int, name: String)
  val users = (1 to 42).map(i => User(i, s"Name$i"))

  // This import enables the use of the Slick sql"...",
  // sqlu"...", and sqlt"..." String interpolators.
  // See "http://slick.lightbend.com/doc/3.2.1/sql.html#string-interpolation"
  import session.profile.api._

  // Stream the users into the database as insert statements
  val done: Future[Done] =
    Source(users)
      .via(
        // add an optional first argument to specify the parallism factor (Int)
        Slick.flow(user => sqlu"INSERT INTO ALPAKKA_SLICK_SCALADSL_TEST_USERS VALUES(${user.id}, ${user.name})")
      )
      .log("nr-of-updated-rows")
      .runWith(Sink.ignore)

  done.onComplete {
    case _ =>
      session.close()
      system.terminate()
  }
}
//#flow-example
