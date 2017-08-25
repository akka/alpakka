/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.neo4j.internal.codec

sealed trait NeoElement
case class NeoRecord(name: Symbol) extends NeoElement
case class NeoField(name: Symbol, value: AnyVal) extends NeoElement
