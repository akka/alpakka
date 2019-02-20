/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import org.apache.mina.util.AvailablePortFinder;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

public abstract class FtpBaseSupport implements FtpSupport, AkkaSupport {

  private static final int BASE_PORT = 21000;
  private static final int DEFAULT_NUM_FILES = 30;
  public static final String hostname = "localhost";
  public static final String FTP_ROOT_DIR = "/home";

  private File usersFile;
  private FileSystem fileSystem;
  private ActorSystem system;
  private Materializer materializer;

  private Integer port;

  private String loremIpsum =
      "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Praesent auctor imperdiet "
          + "velit, eu dapibus nisl dapibus vitae. Sed quam lacus, fringilla posuere ligula at, "
          + "aliquet laoreet nulla. Aliquam id fermentum justo. Aliquam et massa consequat, "
          + "pellentesque dolor nec, gravida libero. Phasellus elit eros, finibus eget "
          + "sollicitudin ac, consectetur sed ante. Etiam ornare lacus blandit nisi gravida "
          + "accumsan. Sed in lorem arcu. Vivamus et eleifend ligula. Maecenas ut commodo ante. "
          + "Suspendisse sit amet placerat arcu, porttitor sagittis velit. Quisque gravida mi a "
          + "porttitor ornare. Cras lorem nisl, sollicitudin vitae odio at, vehicula maximus "
          + "mauris. Sed ac purus ac turpis pellentesque cursus ac eget est. Pellentesque "
          + "habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas.";

  FtpBaseSupport() {
    try {
      usersFile = new File(getClass().getClassLoader().getResource("users.properties").getFile());
      port = AvailablePortFinder.getNextAvailable(BASE_PORT);
      system = ActorSystem.create("alpakka-ftp");
      materializer = ActorMaterializer.create(system);
    } finally {
      port = BASE_PORT;
    }
  }

  public void cleanFiles() {
    for (Path rootDir : getFileSystem().getRootDirectories()) {
      try {
        Files.walkFileTree(
            rootDir,
            new SimpleFileVisitor<Path>() {
              @Override
              public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                  throws IOException {
                Files.deleteIfExists(file);
                return FileVisitResult.CONTINUE;
              }
            });
      } catch (Throwable t) {
        t.printStackTrace();
      }
    }
  }

  public void generateFiles(int numFiles, int pageSize, String basePath) {
    String base = "";
    if (!basePath.isEmpty()) {
      if ('/' != basePath.charAt(0)) {
        base = "/" + basePath;
      } else {
        base = basePath;
      }
    }
    int i = 1;
    while (i <= numFiles) {
      int j = i / pageSize;
      String subDir = (j > 0) ? "/dir_" + j : "";
      putFileOnFtp(FTP_ROOT_DIR + base + subDir, "sample_" + i);
      i++;
    }
  }

  public void putFileOnFtp(String path, String fileName) {
    putFileOnFtpWithContents(path, fileName, loremIpsum.getBytes());
  }

  public void putFileOnFtpWithContents(String path, String fileName, byte[] fileContents) {
    try {
      Path baseDir = getFileSystem().getPath(path);
      if (!Files.exists(baseDir)) {
        Files.createDirectories(baseDir);
      }
      Path filePath = baseDir.resolve(fileName);
      Files.write(filePath, fileContents);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  public byte[] getFtpFileContents(String path, String fileName) {
    try {
      Path baseDir = getFileSystem().getPath(path);
      Path filePath = baseDir.resolve(fileName);
      return Files.readAllBytes(filePath);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  public boolean fileExists(String path, String fileName) {
    try {
      Path baseDir = getFileSystem().getPath(path);
      Path filePath = baseDir.resolve(fileName);
      return Files.exists(filePath);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  public ActorSystem getSystem() {
    return system;
  }

  public Materializer getMaterializer() {
    return materializer;
  }

  protected File getUsersFile() {
    return usersFile;
  }

  protected FileSystem getFileSystem() {
    return fileSystem;
  }

  protected void setFileSystem(FileSystem fileSystem) {
    this.fileSystem = fileSystem;
  }

  protected Integer getPort() {
    return port;
  }

  public String getLoremIpsum() {
    return loremIpsum;
  }
}
