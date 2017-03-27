/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp;

interface FtpSupport {

    void startServer();

    void stopServer();

    void cleanFiles();

    void generateFiles();

    void generateFiles(int numFiles);

    void generateFiles(int numFiles, int pageSize);

    void generateFiles(int numFiles, int pageSize, String basePath);

    void putFileOnFtp(String path, String fileName);

    void putFileOnFtpWithContents(String path, String fileName, byte[] fileContents);

    byte[] getFtpFileContents(String path, String fileName);

    String getLoremIpsum();
}
