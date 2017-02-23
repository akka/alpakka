package akka.stream.alpakka.sns

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.concurrent.Await
import scala.concurrent.duration._

trait DefaultTestContext extends BeforeAndAfterAll with MockitoSugar { this: Suite =>

  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: Materializer = ActorMaterializer()

  override protected def afterAll(): Unit =
    Await.ready(system.terminate(), 5.seconds)

}
