/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp.examples;

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
