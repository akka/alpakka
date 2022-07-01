package akka.stream.alpakka.typesense.json

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should
import spray.json._

abstract class TypesenseJsonProtocolSpec extends AnyFunSpec with should.Matchers {
  protected def checkJson[T](data: T, expectedJson: JsObject)(implicit jsonFormat: RootJsonFormat[T]): Unit = {
    data.toJson shouldBe expectedJson
    expectedJson.convertTo[T] shouldBe data
  }
}
