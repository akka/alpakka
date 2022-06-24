package akka.stream.alpakka.typesense

import akka.actor.ActorSystem
import akka.stream.alpakka.typesense.impl.TypesenseHttp
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import com.dimafeng.testcontainers.{DockerComposeContainer, ExposedService}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatest.time.{Millis, Seconds, Span}
import org.testcontainers.containers.wait.strategy.Wait

import java.io.File

class TypesenseSpec extends AnyFlatSpec with TestContainerForAll with ScalaFutures with should.Matchers {
  implicit val system: ActorSystem = ActorSystem()
  implicit val defaultPatience: PatienceConfig = PatienceConfig(timeout = Span(5, Seconds), interval = Span(5, Millis))

  val dockerComposeFile: File = new File("typesense/src/test/resources/docker-compose.yml")
  val port = 8108
  val containerName = "typesense"
  val exposedService: ExposedService =
    ExposedService(containerName, port, Wait.forHttp("/collections").forStatusCode(401))
  val apiKey = "Hu52dwsas2AdxdE"
  val settings: TypesenseSettings = TypesenseSettings(s"http://localhost:$port", apiKey)

  override val containerDef: DockerComposeContainer.Def =
    DockerComposeContainer.Def(dockerComposeFile, Seq(exposedService))

  "Collection" should "be created" in {
    val fields = List(Field("id", "string"), Field("name", "string"))
    val schema = CollectionSchema("my-collection", fields)
    val res = TypesenseHttp.createCollectionRequest(schema, settings)(system, system.dispatcher).futureValue
    val expectedResponse = CollectionResponse("my-collection", 0, fields)
    res shouldBe expectedResponse
  }
}
