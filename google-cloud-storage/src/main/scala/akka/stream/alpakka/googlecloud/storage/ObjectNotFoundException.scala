/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.googlecloud.storage

final class ObjectNotFoundException(err: String) extends RuntimeException(err)
