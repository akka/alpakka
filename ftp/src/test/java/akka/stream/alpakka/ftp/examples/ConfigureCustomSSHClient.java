/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp.examples;

//#configure-custom-ssh-client

import akka.stream.alpakka.ftp.javadsl.FtpApi;
import akka.stream.alpakka.ftp.javadsl.Sftp;
import net.schmizz.sshj.DefaultConfig;
import net.schmizz.sshj.SSHClient;

public class ConfigureCustomSSHClient {

    public ConfigureCustomSSHClient() {
        SSHClient sshClient = new SSHClient(new DefaultConfig());
        FtpApi<SSHClient> sftp = Sftp.create(sshClient);

    }
}
//#configure-custom-ssh-client