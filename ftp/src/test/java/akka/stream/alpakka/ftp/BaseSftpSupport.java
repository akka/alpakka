/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;

public class BaseSftpSupport extends BaseSupportImpl {

  private final Path ROOT_DIR = Paths.get("tmp/home");
  final String HOSTNAME = "localhost";
  final int PORT = 2222;
  final FtpCredentials CREDENTIALS = FtpCredentials.create("username", "userpass");
  // Issue: the root folder of the sftp server is not writable so tests must happen inside a
  // sub-folder
  final String ROOT_PATH = "upload/";

  public static final byte[] CLIENT_PRIVATE_KEY_PASSPHRASE =
      "secret".getBytes(Charset.forName("UTF-8"));

  private File clientPrivateKeyFile;
  private File knownHostsFile;

  BaseSftpSupport() {
    clientPrivateKeyFile = new File(getClass().getResource("/id_rsa").getPath());
    knownHostsFile = new File(getClass().getResource("/known_hosts").getPath());
  }

  public File getClientPrivateKeyFile() {
    return clientPrivateKeyFile;
  }

  public File getKnownHostsFile() {
    return knownHostsFile;
  }

  @Override
  public Path getRootDir() {
    return ROOT_DIR;
  }
}
