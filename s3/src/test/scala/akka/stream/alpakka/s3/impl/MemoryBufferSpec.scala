package akka.stream.alpakka.s3.impl

import scala.Vector

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpecLike
import org.scalatest.Matchers
import org.scalatest.concurrent.ScalaFutures

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.ActorMaterializerSettings
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.testkit.TestKit
import akka.util.ByteString

class MemoryBufferSpec (_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with Matchers with BeforeAndAfterAll with ScalaFutures {

  def this() = this(ActorSystem("MemoryBufferSpec"))

  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withDebugLogging(true))
  
  "MemoryBuffer" should "emit a chunk on its output containg the concatenation of all input values" in {
    val result = Source(Vector(ByteString(1,2,3,4,5), ByteString(6,7,8,9,10,11,12), ByteString(13,14)))
      .via(new MemoryBuffer(200))
      .runWith(Sink.seq)
      .futureValue
      
    result should have size (1)
    val chunk = result.head
    chunk.size should be (14)
    chunk.data.runWith(Sink.seq).futureValue should be (Seq(ByteString(1,2,3,4,5,6,7,8,9,10,11,12,13,14)))
  }
  
  it should "fail if more than maxSize bytes are fed into it" in {
    whenReady(
      Source(Vector(ByteString(1,2,3,4,5), ByteString(6,7,8,9,10,11,12), ByteString(13,14)))
        .via(new MemoryBuffer(10))
        .runWith(Sink.seq)
        .failed
    ) { e =>
      e shouldBe a[IllegalStateException]
    }    
  }
}