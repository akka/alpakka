/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package docs.javadsl;

// #configure-custom-ssh-client

import akka.stream.alpakka.ftp.javadsl.Sftp;
import akka.stream.alpakka.ftp.javadsl.SftpApi;
import net.schmizz.sshj.DefaultConfig;
import net.schmizz.sshj.SSHClient;

public class ConfigureCustomSSHClient {

  public ConfigureCustomSSHClient() {
    SSHClient sshClient = new SSHClient(new DefaultConfig());
    SftpApi sftp = Sftp.create(sshClient);
  }
}
// #configure-custom-ssh-client
