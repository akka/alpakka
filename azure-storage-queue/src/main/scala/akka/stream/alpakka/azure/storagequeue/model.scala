/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.azure.storagequeue

sealed abstract class DeleteOrUpdateMessage
object DeleteOrUpdateMessage {

  sealed abstract class Delete extends DeleteOrUpdateMessage
  case object Delete extends Delete

  /**
   * Java API
   */
  def createDelete(): Delete = Delete

  final class UpdateVisibility private (val timeout: Int) extends DeleteOrUpdateMessage {
    override def toString: String =
      s"DeleteOrUpdateMessage.UpdateVisibility(timeout=$timeout)"
  }

  object UpdateVisibility {
    def apply(timeout: Int) =
      new UpdateVisibility(timeout)
  }

  /**
   * Java API
   */
  def createUpdateVisibility(timeout: Int): UpdateVisibility = UpdateVisibility(timeout)
}
