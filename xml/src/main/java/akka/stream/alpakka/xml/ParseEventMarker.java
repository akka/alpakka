/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.xml;

/**
 * Mirrors the sub-classes of [[ParseEvent]] to allow use with Java switch statements instead of
 * chained `instanceOf` tests.
 */
public enum ParseEventMarker {
  XMLStartDocument,
  XMLEndDocument,
  XMLStartElement,
  XMLEndElement,
  XMLCharacters,
  XMLProcessingInstruction,
  XMLComment,
  XMLCData,
}
