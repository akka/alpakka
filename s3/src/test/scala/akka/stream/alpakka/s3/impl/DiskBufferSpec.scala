package akka.stream.alpakka.s3.impl

import java.nio.BufferOverflowException
import java.nio.file.Files

import scala.Vector

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpecLike
import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.ScalaFutures

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.ActorMaterializerSettings
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.testkit.TestKit
import akka.util.ByteString

class DiskBufferSpec (_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with Matchers with BeforeAndAfterAll with ScalaFutures with Eventually {

  def this() = this(ActorSystem("DiskBufferSpec"))

  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withDebugLogging(true))
  
  "DiskBuffer" should "emit a chunk on its output containg the concatenation of all input values" in {
    val result = Source(Vector(ByteString(1,2,3,4,5), ByteString(6,7,8,9,10,11,12), ByteString(13,14)))
      .via(new DiskBuffer(1, 200, None))
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
      .via(new DiskBuffer(1, 10, None))
        .runWith(Sink.seq)
        .failed
    ) { e =>
      e shouldBe a[BufferOverflowException]
    }    
  }

  it should "delete its temp file after N materializations" in {
    val tmpDir = Files.createTempDirectory("DiskBufferSpec")
    val before = tmpDir.toFile().list().size
    val source = Source(Vector(ByteString(1,2,3,4,5,6,7,8,9,10,11,12,13,14)))
      .via(new DiskBuffer(2, 200, Some(tmpDir)))
      .runWith(Sink.seq)
      .futureValue
      .head
      .data
    
    tmpDir.toFile().list().size should be (before + 1)
    
    source.runWith(Sink.ignore).futureValue
    tmpDir.toFile().list().size should be (before + 1)
    
    source.runWith(Sink.ignore).futureValue
    eventually {
      tmpDir.toFile().list().size should be (before)      
    }
    
  }
}