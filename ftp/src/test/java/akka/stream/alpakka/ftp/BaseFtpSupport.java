/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp;

import java.nio.file.Path;
import java.nio.file.Paths;

public class BaseFtpSupport extends BaseSupportImpl {

  private final Path ROOT_DIR = Paths.get("target/home");
  public final String HOSTNAME = "localhost";
  public final int PORT = 21000;
  public final FtpCredentials CREDENTIALS = FtpCredentials.create("username", "userpass");

  @Override
  public Path getRootDir() {
    return ROOT_DIR;
  }
}
