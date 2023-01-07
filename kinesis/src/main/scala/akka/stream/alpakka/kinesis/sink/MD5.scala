package akka.stream.alpakka.kinesis.sink

import java.security.MessageDigest

object MD5 {
  private val threadLocalMD5 = ThreadLocal.withInitial(() => MessageDigest.getInstance("MD5"))

  def threadLocal[T](f: MessageDigest => T): T = {
    val messageDigest = threadLocalMD5.get()
    try f(messageDigest)
    finally messageDigest.reset()
  }
}
