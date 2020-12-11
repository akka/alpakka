/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.ftp;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import org.junit.After;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

public abstract class BaseSupportImpl implements BaseSupport, AkkaSupport {

  private ActorSystem system = ActorSystem.create("alpakka-ftp");
  private Materializer materializer = ActorMaterializer.create(system);
  public final FtpCredentials CREDENTIALS = FtpCredentials.create("username", "userpass");
  public final FtpCredentials WRONG_CREDENTIALS = FtpCredentials.create("username", "qwerty");

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

  abstract Path getRootDir();

  @Override
  @After
  public void cleanFiles() {
    try {
      Files.walkFileTree(
          getRootDir(),
          new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                throws IOException {
              if (!dir.equals(getRootDir())) Files.delete(dir);
              return FileVisitResult.CONTINUE;
            }

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

  @Override
  public void generateFiles(int numFiles, int pageSize, String basePath) {
    String path = basePath.endsWith("/") ? basePath.substring(0, basePath.length() - 1) : basePath;
    int i = 1;
    while (i <= numFiles) {
      int j = i / pageSize;
      String subDir = (j > 0) ? "dir_" + j + "/" : "";
      putFileOnFtp(path + "/" + subDir + "sample_" + i);
      i++;
    }
  }

  @Override
  public void putFileOnFtp(String filePath) {
    putFileOnFtpWithContents(filePath, getDefaultContent().getBytes());
  }

  @Override
  public void putFileOnFtpWithContents(String filePath, byte[] fileContents) {
    String relativePath = filePath.startsWith("/") ? filePath.substring(1) : filePath;
    try {
      File parent = getRootDir().resolve(relativePath).getParent().toFile();
      if (!parent.exists()) {
        parent.mkdirs();
      }
      Path path = getRootDir().resolve(relativePath);
      path.toFile().createNewFile();
      Files.write(path, fileContents, StandardOpenOption.SYNC);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  @Override
  public byte[] getFtpFileContents(String filePath) {
    String relativePath = filePath.startsWith("/") ? filePath.substring(1) : filePath;
    try {
      Path path = getRootDir().resolve(relativePath);
      return Files.readAllBytes(path);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  @Override
  public boolean fileExists(String filePath) {
    String relativePath = filePath.startsWith("/") ? filePath.substring(1) : filePath;
    try {
      return getRootDir().resolve(relativePath).toFile().exists();
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  @Override
  public String getDefaultContent() {
    return loremIpsum;
  }

  @Override
  public ActorSystem getSystem() {
    return system;
  }

  @Override
  public Materializer getMaterializer() {
    return materializer;
  }
}
