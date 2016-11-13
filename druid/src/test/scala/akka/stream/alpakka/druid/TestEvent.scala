/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.druid

case class TestEvent(timestamp: String, page: String, added: Int, tags: List[String])
