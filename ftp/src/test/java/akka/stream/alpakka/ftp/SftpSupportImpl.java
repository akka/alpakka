/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.auth.password.PasswordAuthenticator;
import org.apache.sshd.server.auth.password.PasswordChangeRequiredException;
import org.apache.sshd.server.auth.pubkey.PublickeyAuthenticator;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.scp.ScpCommandFactory;
import org.apache.sshd.server.session.ServerSession;
import org.apache.sshd.server.subsystem.sftp.SftpSubsystemFactory;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PublicKey;
import java.util.Arrays;

abstract class SftpSupportImpl extends FtpBaseSupport {

    private SshServer sshd;
    private File keyPairProviderFile;

    SftpSupportImpl() {
        keyPairProviderFile =
                new File("ftp/src/test/resources/hostkey.pem");
    }

    @Before
    public void startServer() {
        try {
            sshd = SshServer.setUpDefaultServer();
            sshd.setPort(getPort());
            sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider(keyPairProviderFile));
            sshd.setSubsystemFactories(Arrays.<NamedFactory<Command>>asList(new SftpSubsystemFactory()));
            sshd.setCommandFactory(new ScpCommandFactory());
            PasswordAuthenticator passwordAuthenticator = new PasswordAuthenticator() {
                public boolean authenticate(String username, String password, ServerSession session) throws PasswordChangeRequiredException {
                    return (username != null && username.equals(password));
                }
            };
            sshd.setPasswordAuthenticator(passwordAuthenticator);
            PublickeyAuthenticator publickeyAuthenticator = new PublickeyAuthenticator() {
                @Override
                public boolean authenticate(String username, PublicKey key, ServerSession session) {
                    return true;
                }
            };
            sshd.setPublickeyAuthenticator(publickeyAuthenticator);

            // setting the virtual filesystem.
            // posix attribute view is necessary in order to
            // avoid jimfs relying to `toFile` method in RootedPath
            // (throws UnsupportedOperationException)
            Configuration fsConfig = Configuration.unix().toBuilder().setAttributeViews("basic", "posix").build();
            setFileSystem(Jimfs.newFileSystem(fsConfig));
            Path home = getFileSystem().getPath(FTP_ROOT_DIR);
            sshd.setFileSystemFactory(new VirtualFileSystemFactory(home));

            // start
            sshd.start();

            // create home dir
            if (!Files.exists(home)) {
                Files.createDirectories(home);
            }

        } catch(Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @After
    public void stopServer() {
        try {
            sshd.stop(true);
        } catch(Throwable t) {
            throw new RuntimeException(t);
        }
    }
}
