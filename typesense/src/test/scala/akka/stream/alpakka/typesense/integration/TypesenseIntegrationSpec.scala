package akka.stream.alpakka.typesense.integration

import akka.actor.ActorSystem
import akka.stream.alpakka.typesense.TypesenseSettings
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import com.dimafeng.testcontainers.{DockerComposeContainer, ExposedService}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should
import org.scalatest.time.{Millis, Seconds, Span}
import org.testcontainers.containers.wait.strategy.Wait

import java.io.File

/**
 * Integration tests for Typesense using Docker container.
 * The idea is to execute tests for two Typesense versions:
 * the newest one (0.23.0) and the oldest with available documentation (0.11.2)
 */
abstract class TypesenseIntegrationSpec(protected val version: String)
    extends AnyFunSpec
    with TestContainerForAll
    with ScalaFutures
    with should.Matchers {
  implicit val system: ActorSystem = ActorSystem()
  implicit val defaultPatience: PatienceConfig = PatienceConfig(timeout = Span(5, Seconds), interval = Span(5, Millis))

  private val versionFileSufix = version.replace('.', '_')
  private val dockerComposeFile: File = new File(s"typesense/src/test/resources/docker-compose-$versionFileSufix.yml")
  private val port = 8108
  private val containerName = "typesense"
  private val exposedService: ExposedService =
    ExposedService(containerName, port, Wait.forHttp("/collections").forStatusCode(401))
  private val apiKey = "Hu52dwsas2AdxdE"

  protected val settings: TypesenseSettings = TypesenseSettings(s"http://localhost:$port", apiKey)

  override val containerDef: DockerComposeContainer.Def =
    DockerComposeContainer.Def(dockerComposeFile, Seq(exposedService))
}
