/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
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
