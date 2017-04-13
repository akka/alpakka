/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.druid

import com.metamx.tranquility.partition.Partitioner
import com.metamx.tranquility.typeclass.{ObjectWriter, Timestamper}
import io.circe.Encoder
import org.joda.time.DateTime

object DruidTestCodec {
  implicit val timestamper = new Timestamper[TestEvent] {
    def timestamp(a: TestEvent): DateTime = new DateTime(a.timestamp)
  }

  implicit val testWriter = new ObjectWriter[TestEvent] {
    import io.circe.generic.semiauto._
    import io.circe.syntax._
    implicit val encoder: Encoder[TestEvent] = deriveEncoder[TestEvent]

    /**
     * Serialize a single object. When serializing to JSON, this should result in a JSON object.
     */
    def asBytes(obj: TestEvent): Array[Byte] = obj.asJson.noSpaces.getBytes

    /**
     * Serialize a batch of objects to send all at once. When serializing to JSON, this should result in a JSON array
     * of objects.
     */
    def batchAsBytes(objects: TraversableOnce[TestEvent]): Array[Byte] =
      ("[" + objects.map(_.asJson.noSpaces).mkString(",") + "]").getBytes

    /**
     * @return content type of the serialized form
     */
    def contentType: String = "application/json"
  }

  implicit val partitioner = new Partitioner[TestEvent] {
    override def partition(a: TestEvent, numPartitions: Int): Int = a.page.hashCode % numPartitions
  }
}
