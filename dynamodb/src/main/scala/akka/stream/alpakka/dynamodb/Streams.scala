/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.dynamodb

import akka.NotUsed
import akka.stream.alpakka.dynamodb.scaladsl.DynamoClient
import akka.stream.scaladsl.Source
import com.amazonaws.services.dynamodbv2.model._
import akka.stream.alpakka.dynamodb.scaladsl.DynamoImplicits._

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConverters._

object Streams {

  def records(tableName: String)(implicit client: DynamoClient,
                                 executionContext: ExecutionContext): Source[Record, NotUsed] =
    client
      .source(new DescribeTableRequest().withTableName(tableName))
      .map(describeTableResult => describeTableResult.getTable.getLatestStreamArn)
      .flatMapConcat { arn =>
        client
          .source(new DescribeStreamRequest().withStreamArn(arn))
          .flatMapConcat { describeStreamResult =>
            val shards = describeStreamResult.getStreamDescription.getShards.asScala

            shardsToRecords(arn, shards.toList)
          }
      }

  def shardsToRecords(
      streamArn: String,
      shards: List[Shard]
  )(implicit client: DynamoClient, executionContext: ExecutionContext): Source[Record, NotUsed] =
    Source.fromIterator(() => shards.toIterator).flatMapConcat { shard => // can we process shards in parallel?
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
