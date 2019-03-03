/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp;

interface FtpSupport {

  void startServer();

  void stopServer();

  void cleanFiles();

  void generateFiles(int numFiles, int pageSize, String basePath);

  void putFileOnFtp(String path, String fileName);

  void putFileOnFtpWithContents(String path, String fileName, byte[] fileContents);

  byte[] getFtpFileContents(String path, String fileName);

  boolean fileExists(String path, String fileName);

  String getLoremIpsum();
}
