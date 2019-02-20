/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package playground.filesystem.impl;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;

public class NameEqualsPathFilter implements DirectoryStream.Filter<Path> {

  private String nameToMatch;

  private boolean caseInsensitive = false;

  public NameEqualsPathFilter(final String nameToMatch,
                              final boolean caseInsensitive) {
    this.nameToMatch = nameToMatch;
    this.caseInsensitive = caseInsensitive;
  }

  @Override
  public boolean accept(Path entry) throws IOException {
    if (caseInsensitive) {
     return entry.getFileName().toString().equalsIgnoreCase(nameToMatch);
    } else {
      return entry.getFileName().toString().equals(nameToMatch);
    }
  }
}
