/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.ftp;

interface BaseSupport {

  void cleanFiles();

  void generateFiles(int numFiles, int pageSize, String basePath);

  void putFileOnFtp(String filePath);

  void putFileOnFtpWithContents(String filePath, byte[] fileContents);

  byte[] getFtpFileContents(String filePath);

  boolean fileExists(String filePath);

  String getDefaultContent();
}
