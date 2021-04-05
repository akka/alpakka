/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.logging

import akka.http.scaladsl.model.{ErrorInfo, ExceptionWithErrorInfo}

final case class CloudLoggingException private (override val info: ErrorInfo) extends ExceptionWithErrorInfo(info)
