/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
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

//#flowWithPassThrough-example
import scala.concurrent.Future

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

import akka.stream.scaladsl._
import akka.stream.alpakka.slick.scaladsl._

// We're going to pretend we got messages from kafka.
// After we've written them to a db with Slick, we want
// to commit the offset to Kafka
object SlickFlowWithPassThroughExample extends App {

  // mimics a Kafka 'Committable' type
  case class CommittableOffset(offset: Int) {
    def commit: Future[Done] = Future.successful(Done)
  }
  case class KafkaMessage[A](msg: A, offset: CommittableOffset) {
    // map the msg and keep the offset
    def map[B](f: A => B): KafkaMessage[B] = KafkaMessage(f(msg), offset)
  }

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  implicit val ec = system.dispatcher

  implicit val session = SlickSession.forConfig("slick-h2")

  // The example domain
  case class User(id: Int, name: String)
  val users = (1 to 42).map(i => User(i, s"Name$i"))
  val messagesFromKafka = users.zipWithIndex.map { case (user, index) => KafkaMessage(user, CommittableOffset(index)) }

  // This import enables the use of the Slick sql"...",
  // sqlu"...", and sqlt"..." String interpolators.
  // See "http://slick.lightbend.com/doc/3.2.1/sql.html#string-interpolation"
  import session.profile.api._

  // Stream the users into the database as insert statements
  val done: Future[Done] =
    Source(messagesFromKafka)
      .via(
        // add an optional first argument to specify the parallism factor (Int)
        Slick.flowWithPassThrough { kafkaMessage =>
          val user = kafkaMessage.msg
          (sqlu"INSERT INTO ALPAKKA_SLICK_SCALADSL_TEST_USERS VALUES(${user.id}, ${user.name})")
            .map { insertCount => // map db result to something else
              // allows to keep the kafka message offset so it can be committed in a next stage
              kafkaMessage.map(user => (user, insertCount))
            }
        }
      )
      .log("nr-of-updated-rows")
      .mapAsync(1) { // in correct order
        kafkaMessage =>
          kafkaMessage.offset.commit // commit kafka messages
      }
      .runWith(Sink.ignore)

  done.onComplete {
    case _ =>
      session.close()
      system.terminate()
  }
}
//#flowWithPassThrough-example
