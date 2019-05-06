/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.testkit
import java.util.Optional

import akka.annotation.ApiMayChange
import akka.stream.alpakka.sqs.{FifoMessageIdentifiers, MessageAction, SqsAckResult, SqsPublishResult}
import software.amazon.awssdk.services.sqs.model.SqsResponse

import scala.compat.java8.OptionConverters._

/**
 * Message factory class for testing purposes
 */
object MessageFactory {
  @ApiMayChange
  def createFifoMessageIdentifiers(sequenceNumber: String,
                                   messageGroupId: String,
                                   messageDeduplicationId: Option[String]): FifoMessageIdentifiers =
    new FifoMessageIdentifiers(sequenceNumber, messageGroupId, messageDeduplicationId)

  @ApiMayChange
  def createSqsPublishResult[T <: SqsResponse](
      metadata: T,
      fifoMessageIdentifiers: Option[FifoMessageIdentifiers]
  ): SqsPublishResult[T] =
    new SqsPublishResult(metadata, fifoMessageIdentifiers)

  @ApiMayChange
  def createSqsAckResult[T <: SqsResponse](metadata: Option[T], messageAction: MessageAction): SqsAckResult[T] =
    new SqsAckResult(metadata, messageAction)

  /** Java API */
  @ApiMayChange
  def createFifoMessageIdentifiers(sequenceNumber: String,
                                   messageGroupId: String,
                                   messageDeduplicationId: Optional[String]): FifoMessageIdentifiers =
    new FifoMessageIdentifiers(sequenceNumber, messageGroupId, messageDeduplicationId.asScala)

  /** Java API */
  @ApiMayChange
  def createSqsPublishResult[T <: SqsResponse](
      metadata: T,
      fifoMessageIdentifiers: Optional[FifoMessageIdentifiers]
  ): SqsPublishResult[T] =
    new SqsPublishResult(metadata, fifoMessageIdentifiers.asScala)

  /** Java API */
  @ApiMayChange
  def createSqsAckResult[T <: SqsResponse](metadata: Optional[T], messageAction: MessageAction): SqsAckResult[T] =
    new SqsAckResult(metadata.asScala, messageAction)

}
