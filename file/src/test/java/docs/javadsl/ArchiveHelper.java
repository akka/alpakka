/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.util.ByteString;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ArchiveHelper {

  public Map<String, ByteString> unzip(ByteString zipArchive) throws Exception {
    ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipArchive.toArray()));
    ZipEntry entry;
    Map<String, ByteString> result = new HashMap<>();
    try {
      while ((entry = zis.getNextEntry()) != null) {
        int count;
        byte[] data = new byte[1024];

        ByteArrayOutputStream dest = new ByteArrayOutputStream();
        while ((count = zis.read(data, 0, 1024)) != -1) {
          dest.write(data, 0, count);
        }
        dest.flush();
        dest.close();
        zis.closeEntry();
        result.putIfAbsent(entry.getName(), ByteString.fromArray(dest.toByteArray()));
      }
    } finally {
      zis.close();
    }
    return result;
  }
}
