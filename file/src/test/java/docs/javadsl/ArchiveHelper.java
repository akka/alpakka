/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ArchiveHelper {

  public void createReferenceZipFile(List<Path> inputFilePaths, String resultFileName)
      throws Exception {
    FileOutputStream fout = new FileOutputStream(resultFileName);
    ZipOutputStream zout = new ZipOutputStream(fout);
    for (Path inputFilePath : inputFilePaths) {
      File fileToZip = inputFilePath.toFile();
      FileInputStream fis = new FileInputStream(fileToZip);
      ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
      zout.putNextEntry(zipEntry);
      byte[] bytes = new byte[1024];
      int length;
      while ((length = fis.read(bytes)) >= 0) {
        zout.write(bytes, 0, length);
      }
      fis.close();
    }
    zout.close();
  }
}
