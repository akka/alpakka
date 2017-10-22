/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.dynamodb.impl

import akka.NotUsed
import akka.stream.alpakka.dynamodb.scaladsl.DynamoClient
import akka.stream.alpakka.dynamodb.scaladsl.DynamoImplicits._
import akka.stream.scaladsl.Source
import com.amazonaws.services.dynamodbv2.model._
import java.util.{List => JList}
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

object Streams {

  /**
   * Produces a Source of Dynamodb Streams records for the table returned by the describeTableRequest.
   */
  def records(describeTableRequest: DescribeTableRequest)(implicit client: DynamoClient,
                                                          executionContext: ExecutionContext): Source[Record, NotUsed] =
    client
      .source(describeTableRequest)
      .map(describeTableResult => describeTableResult.getTable.getLatestStreamArn)
      .flatMapConcat { arn =>
        client
          .source(new DescribeStreamRequest().withStreamArn(arn))
          .flatMapConcat { describeStreamResult =>
            val shards = describeStreamResult.getStreamDescription.getShards

            shardsToRecords(arn, shards)
          }
      }

  def shardsToRecords(
      streamArn: String,
      shards: JList[Shard]
  )(implicit client: DynamoClient, executionContext: ExecutionContext): Source[Record, NotUsed] =
    Source.fromIterator(() => shards.iterator().asScala).flatMapConcat { shard => // can we process shards in parallel?
      val parent = shard.getParentShardId
      println(s"parent $parent") // should we process the parent first if it's not null?
      client
        .source(getShardIteratorRequest(streamArn = streamArn, shardId = shard.getShardId).toOp)
        .flatMapConcat { //results need to be in the same order
          result =>
            Source.unfoldAsync(Option(result.getShardIterator)) {
              case None => Future.successful(None)
              case Some(nextShardIterator) =>
                client.single(new GetRecordsRequest().withShardIterator(nextShardIterator)).map { result =>
                  Some((Option(result.getNextShardIterator), result.getRecords))
                }
            }
        }
        .flatMapConcat(records => Source.fromIterator(() => records.asScala.toIterator))
    }

  def getShardIteratorRequest(streamArn: String, shardId: String) =
    new GetShardIteratorRequest()
      .withStreamArn(streamArn)
      .withShardId(shardId)
      .withShardIteratorType(ShardIteratorType.TRIM_HORIZON)

}
