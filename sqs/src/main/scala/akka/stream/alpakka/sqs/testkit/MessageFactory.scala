/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.testkit

import akka.annotation.ApiMayChange
import akka.stream.alpakka.sqs.{FifoMessageIdentifiers, MessageAction, SqsAckResult, SqsPublishResult}
import com.amazonaws.{AmazonWebServiceResult, ResponseMetadata}
import com.amazonaws.services.sqs.model.Message

import scala.compat.java8.OptionConverters._

object MessageFactory {

  /**
   * Scala API
   * For use with testing
   */
  @ApiMayChange
  def createFifoMessageIdentifiers(
      sequenceNumber: String,
      messageGroupId: String,
      messageDeduplicationId: Option[String]
  ): FifoMessageIdentifiers = new FifoMessageIdentifiers(
    sequenceNumber,
    messageGroupId,
    messageDeduplicationId
  )

  /**
   * Java API
   * For use with testing
   */
  @ApiMayChange
  def createFifoMessageIdentifiers(
      sequenceNumber: String,
      messageGroupId: String,
      messageDeduplicationId: java.util.Optional[String]
  ): FifoMessageIdentifiers = new FifoMessageIdentifiers(
    sequenceNumber,
    messageGroupId,
    messageDeduplicationId.asScala
  )

  /**
   * Scala API
   * For use with testing
   */
  @ApiMayChange
  def createSqsPublishResult(
      metadata: AmazonWebServiceResult[ResponseMetadata],
      message: Message,
      fifoMessageIdentifiers: Option[FifoMessageIdentifiers]
  ): SqsPublishResult = new SqsPublishResult(
    metadata,
    message,
    fifoMessageIdentifiers
  )

  /**
   * Java API
   * For use with testing
   */
  @ApiMayChange
  def SqsPublishResult(
      metadata: AmazonWebServiceResult[ResponseMetadata],
      message: Message,
      fifoMessageIdentifiers: java.util.Optional[FifoMessageIdentifiers]
  ): SqsPublishResult = new SqsPublishResult(
    metadata,
    message,
    fifoMessageIdentifiers.asScala
  )

  /**
   * Scala API
   * For use with testing
   */
  @ApiMayChange
  def createSqsAckResult(
      metadata: Option[com.amazonaws.AmazonWebServiceResult[com.amazonaws.ResponseMetadata]],
      messageAction: MessageAction
  ): SqsAckResult = new SqsAckResult(
    metadata,
    messageAction
  )

  /**
   * Java API
   * For use with testing
   */
  @ApiMayChange
  def createSqsAckResult(
      metadata: java.util.Optional[com.amazonaws.AmazonWebServiceResult[com.amazonaws.ResponseMetadata]],
      messageAction: MessageAction
  ): SqsAckResult = new SqsAckResult(
    metadata.asScala,
    messageAction
  )
}
