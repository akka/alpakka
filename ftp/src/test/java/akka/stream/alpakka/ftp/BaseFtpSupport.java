/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.ftp;

import java.nio.file.Path;
import java.nio.file.Paths;

public class BaseFtpSupport extends BaseSupportImpl {

  private final Path ROOT_DIR = Paths.get("tmp/home");
  public final String HOSTNAME = "localhost";
  public final int PORT = 21000;

  @Override
  public Path getRootDir() {
    return ROOT_DIR;
  }
}
