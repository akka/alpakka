/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.text.impl

import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset.{Charset, CharsetDecoder, CharsetEncoder}

import akka.annotation.InternalApi
import akka.stream.{FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString

/**
 * ByteBuffer to CharBuffer decoding logic.
 */
@InternalApi
private[impl] trait Decoding {
  protected def decoder: CharsetDecoder

  protected def failStage(exception: Throwable): Unit

  /**
   * Decodes ByteBuffer and calls #decoded with results.
   */
  protected def decode(bytes: ByteBuffer): Unit = {
    val chars = CharBuffer.allocate(bytes.limit())
    val result = decoder.decode(bytes, chars, false)
    if (result.isOverflow) {
      failStage(new IllegalArgumentException(s"Incoming bytes decoded into more characters: $result"))
    } else {
      if (result.isError) {
        result.throwException()
      }
      val count = chars.position()
      chars.rewind()
      decoded(chars, count, bytes)
    }
  }

  /**
   * @param chars the characters successfully decoded
   * @param count number of characters decoded (counted from chars.position)
   * @param bytes remaining non-decoded bytes
   */
  protected def decoded(chars: CharBuffer, count: Int, bytes: ByteBuffer): Unit

}

@InternalApi
private[impl] trait Encoding {
  protected def encoder: CharsetEncoder

  protected def failStage(exception: Throwable): Unit

  /**
   * Creates new CharBuffer containing both Strings.
   */
  protected def toCharBuffer(current: String, input: String): CharBuffer = {
    val chars = CharBuffer.allocate(input.length + current.length)
    chars.append(current)
    chars.append(input)
    chars.rewind
    chars
  }

  /**
   * Decodes CharBuffer and calls #encoded with results.
   */
  protected def encode(chars: CharBuffer): Unit = {
    val bytes = ByteBuffer.allocate((chars.limit() * encoder.maxBytesPerChar().toDouble).toInt)
    val result = encoder.encode(chars, bytes, false)
    if (result.isOverflow) {
      failStage(new IllegalArgumentException(s"Incoming chars decoded into more than expected characters: $result"))
    } else {
      if (result.isError) {
        result.throwException()
      }
      val count = bytes.position()
      bytes.rewind()
      bytes.limit(count)
      if (chars.position() != chars.limit())
        failStage(new IllegalStateException(s"Couldn't encode all characters: left-over '$chars'"))
      encoded(bytes, count)
    }
  }

  /**
   * @param buffer the bytes successfully encoded
   * @param count number of bytes encoded (counted from bytes.position)
   */
  protected def encoded(buffer: ByteBuffer, count: Int): Unit

}

@InternalApi
private[impl] class DecodingLogic(in: Inlet[ByteString],
                                  out: Outlet[String],
                                  shape: FlowShape[ByteString, String],
                                  incoming: Charset)
    extends GraphStageLogic(shape)
    with Decoding
    with InHandler
    with OutHandler {

  protected final val decoder = incoming.newDecoder()
  private var buffer = ByteString.empty

  setHandlers(in, out, this)

  override def onPull(): Unit = pull(in)

  override def onPush(): Unit = decode(buffer.concat(grab(in)).asByteBuffer)

  override def onUpstreamFinish(): Unit = {
    decode(buffer.asByteBuffer)
    if (buffer.nonEmpty)
      throw new IllegalArgumentException(s"Stray bytes at end of input that could not be decoded: $buffer")
    completeStage()
  }

  override protected def decoded(chars: CharBuffer, count: Int, remainingBytes: ByteBuffer): Unit = {
    if (count > 0) push(out, new String(chars.array, chars.position(), count))
    else if (!isClosed(in)) pull(in)
    buffer = ByteString.fromByteBuffer(remainingBytes)
  }

}

private[impl] class EncodingLogic(in: Inlet[String],
                                  out: Outlet[ByteString],
                                  shape: FlowShape[String, ByteString],
                                  outgoing: Charset)
    extends GraphStageLogic(shape)
    with Encoding
    with InHandler
    with OutHandler {

  protected final val encoder = outgoing.newEncoder()

  setHandlers(in, out, this)

  override def onPull(): Unit = pull(in)

  override def onPush(): Unit = {
    val input = grab(in)
    val chars = toCharBuffer("", input)
    encode(chars)
  }

  override def onUpstreamFinish(): Unit = completeStage()

  protected def encoded(bytes: ByteBuffer, count: Int): Unit =
    if (count > 0) push(out, ByteString.fromByteBuffer(bytes))
    else if (!isClosed(in)) pull(in)

}

private[impl] class TranscodingLogic(in: Inlet[ByteString],
                                     out: Outlet[ByteString],
                                     shape: FlowShape[ByteString, ByteString],
                                     incoming: Charset,
                                     outgoing: Charset)
    extends GraphStageLogic(shape)
    with Encoding
    with Decoding
    with InHandler
    with OutHandler {

  protected final val decoder = incoming.newDecoder()
  protected final val encoder = outgoing.newEncoder()
  private var buffer = ByteString.empty

  setHandlers(in, out, this)

  override def onPull(): Unit = pull(in)

  override def onPush(): Unit = decode(buffer.concat(grab(in)).asByteBuffer)

  override def onUpstreamFinish(): Unit = {
    decode(buffer.asByteBuffer)
    if (buffer.nonEmpty)
      throw new IllegalArgumentException(s"Stray bytes at end of input that could not be decoded: $buffer")
    completeStage()
  }

  override protected def decoded(chars: CharBuffer, count: Int, remainingBytes: ByteBuffer): Unit = {
    chars.limit(count)
    encode(chars)
    buffer = ByteString.fromByteBuffer(remainingBytes)
  }

  protected def encoded(bytes: ByteBuffer, count: Int): Unit =
    if (count > 0) push(out, ByteString.fromByteBuffer(bytes))
    else if (!isClosed(in)) pull(in)

}
