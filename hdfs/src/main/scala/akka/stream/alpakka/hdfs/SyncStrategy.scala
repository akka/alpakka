/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs

sealed trait SyncStrategy extends Strategy {
  type S = SyncStrategy
}

object SyncStrategy {

  /*
   * Creates [[CountSyncStrategy]]
   */
  def count(c: Long): SyncStrategy = CountSyncStrategy(0, c)

  /*
   * Creates [[NoSyncStrategy]]
   */
  def none: SyncStrategy = NoSyncStrategy

  private final case class CountSyncStrategy(
      executeCount: Long = 0,
      count: Long
  ) extends SyncStrategy {
    def should(): Boolean = executeCount >= count
    def reset(): SyncStrategy = copy(executeCount = 0)
    def update(offset: Long): SyncStrategy = copy(executeCount = executeCount + 1)
  }

  private case object NoSyncStrategy extends SyncStrategy {
    def should(): Boolean = false
    def reset(): SyncStrategy = this
    def update(offset: Long): SyncStrategy = this
  }

}
