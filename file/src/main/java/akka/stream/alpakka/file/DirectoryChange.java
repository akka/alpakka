/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.file;

/** Enumeration of the possible changes that can happen to a directory */
public enum DirectoryChange {
  Modification,
  Creation,
  Deletion
}
