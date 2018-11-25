/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage.impl
import akka.annotation.InternalApi

@InternalApi
private[impl] final case class TokenApiSettings(url: String, scope: String)
