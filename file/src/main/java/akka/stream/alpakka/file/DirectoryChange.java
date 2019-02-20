/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.file;

/** Enumeration of the possible changes that can happen to a directory */
public enum DirectoryChange {
  Modification,
  Creation,
  Deletion
}
