/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.util.ByteString;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
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

  public void createReferenceZipFileFromMemory(
      Map<String, ByteString> inputFilePaths, String resultFileName) throws Exception {
    FileOutputStream fout = new FileOutputStream(resultFileName);
    ZipOutputStream zout = new ZipOutputStream(fout);
    for (Map.Entry<String, ByteString> file : inputFilePaths.entrySet()) {
      ZipEntry zipEntry = new ZipEntry(file.getKey());
      zout.putNextEntry(zipEntry);
      zout.write(file.getValue().toArray(), 0, file.getValue().toArray().length);
    }
    zout.close();
  }
}
