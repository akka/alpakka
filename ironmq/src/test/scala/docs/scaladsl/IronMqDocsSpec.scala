package docs.scaladsl
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpec}
import org.scalatest.concurrent.ScalaFutures

import scala.util.hashing.MurmurHash3

class IronMqDocsSpec extends WordSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  val config: Config = ConfigFactory.parseString(s"""akka.stream.alpakka.ironmq {
                               |  credentials {
                               |    project-id = "${MurmurHash3.stringHash(System.currentTimeMillis().toString)}"
                               |  }
                               |}
      """.stripMargin).withFallback(ConfigFactory.load())

  implicit val system: ActorSystem = ActorSystem("IronMqDocsSpec", config)
  implicit val materializer: Materializer = ActorMaterializer()

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
  }


}
