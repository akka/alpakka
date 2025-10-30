/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.reference

import akka.annotation.InternalApi
import akka.stream.Attributes
import akka.stream.Attributes.Attribute

object ReferenceAttributes {

  /**
   * Wrap a `Resource` to an attribute so it can be attached to a stream stage.
   */
  def resource(resource: Resource) = Attributes(new ReferenceResourceValue(resource))
}

final class ReferenceResourceValue @InternalApi private[reference] (val resource: Resource) extends Attribute
