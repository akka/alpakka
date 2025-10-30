/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.file;

/** Enumeration of the possible changes that can happen to a directory */
public enum DirectoryChange {
  Modification,
  Creation,
  Deletion
}
