package akka.stream.alpakka.kinesis.sink

import scala.collection.mutable

object Buffer {
  type Factory = () => Buffer

  def backpressure: Factory = () => new DefaultBuffer

  def dropHead: Factory =
    () =>
      new DefaultBuffer {
        override def drop(): Option[Aggregated] = Some(buffer.dequeue())
      }

  private class DefaultBuffer extends Buffer {
    protected val buffer = mutable.Queue.empty[Aggregated]
    override def add(elem: Aggregated): Unit = {
      buffer.enqueue(elem)
    }
    override def peek(): Aggregated = buffer.head
    override def take(): Aggregated = buffer.dequeue()
    override def drop(): Option[Aggregated] = None
    override def isEmpty: Boolean = buffer.isEmpty
  }
}

trait Buffer {

  /** Add an element to the buffer
   */
  def add(elem: Aggregated): Unit

  /** Get the element that will be taken next time without removing it from the buffer
   */
  def peek(): Aggregated

  /** Get the next element and remove it from the buffer
   */
  def take(): Aggregated

  /** Get the next element for dropping and remove it from the buffer.
   * This method will only be called when the buffer is not empty. But
   * the implementation may return None to backpressure the upstream
   */
  def drop(): Option[Aggregated]

  /** Whether the buffer is empty or not
   */
  def isEmpty: Boolean
}
