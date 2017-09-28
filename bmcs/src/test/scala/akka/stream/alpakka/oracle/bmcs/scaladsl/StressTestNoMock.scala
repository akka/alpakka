/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.oracle.bmcs.scaladsl

import java.time
import java.time.Instant
import java.util.UUID

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import org.scalatest.Ignore

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
 * Ignored by default. Tries to upload many files to bmcs.
 */
@Ignore
class StressTestNoMock extends BmcsNoMock {

  "During stress Test the client" should "upload many files correctly" in {
    val numObjectsToUpload = 100
    val chunkSz = 10 * 1024 * 1024
    val srcOfUplods: Source[MultipartUploadResult, NotUsed] = Source
      .fromIterator(() => (1 to numObjectsToUpload).iterator)
      .mapAsyncUnordered(20) { x =>
        val randomObj = s"BmcsNoMockTest-${UUID.randomUUID().toString.take(5)}"
        val uploadSink = client.multipartUpload(bucket, randomObj, chunkSize = chunkSz)
        println(s"starting upload of $randomObj")
        largeSrc.runWith(uploadSink)
      }
      .map { upload =>
        println(s"uploaded object ${upload.objectName}")
        upload
      }

    val sink = Sink.seq[MultipartUploadResult].mapMaterializedValue {
      uploadResultsFuture: Future[Seq[MultipartUploadResult]] =>
        uploadResultsFuture.flatMap { results: Seq[MultipartUploadResult] =>
          Future.successful(results)
        }
    }

    val start = Instant.now()
    val allUplods: Future[Seq[MultipartUploadResult]] = srcOfUplods.runWith(sink)

    val results = Await.ready(allUplods, 10.minutes).futureValue
    val end = Instant.now()

    val duration: time.Duration = time.Duration.between(start, end)

    val lengthFuture: Future[Long] = largeSrc.map(bs => bs.length).runFold(0l)(_ + _)
    val length = Await.ready(lengthFuture, 20.seconds).futureValue

    println(
      s"Time taken = ${duration.getSeconds} seconds. Total uploaded = ${length * numObjectsToUpload / (1024 * 1024)} mb"
    )

    println(s"objects uploaded: ${results.mkString("\n")}")

    results.length should equal(numObjectsToUpload)
  }

}
