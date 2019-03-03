/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package playground.filesystem;

import org.apache.ftpserver.ftplet.FileSystemFactory;
import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import playground.filesystem.impl.JimfsView;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;

public class JimfsFactory implements FileSystemFactory {

    private final Logger LOG = LoggerFactory.getLogger(JimfsFactory.class);

    private FileSystem fileSystem;

    private boolean createHome;

    private boolean caseInsensitive;

    public boolean isCreateHome() {
        return createHome;
    }

    public void setCreateHome(boolean createHome) {
        this.createHome = createHome;
    }

    public boolean isCaseInsensitive() {
        return caseInsensitive;
    }

    public void setCaseInsensitive(boolean caseInsensitive) {
        this.caseInsensitive = caseInsensitive;
    }

    public JimfsFactory(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    @Override
    public FileSystemView createFileSystemView(User user) throws FtpException {
        synchronized (user) {
            // create home if does not exist
            if (createHome) {
                String homeDirStr = user.getHomeDirectory();
                Path homeDir = fileSystem.getPath(homeDirStr);
                if (Files.isRegularFile(homeDir)) {
                    LOG.warn("Not a directory :: " + homeDirStr);
                    throw new FtpException("Not a directory :: " + homeDirStr);
                }
                if (!Files.exists(homeDir)) {
                    try {
                        Files.createDirectories(homeDir);
                    } catch (IOException t) {
                        final String msg = "Cannot create user home :: " + homeDirStr;
                        LOG.warn(msg);
                        throw new FtpException(msg, t);
                    }
                }
            }
            return new JimfsView(fileSystem, user, caseInsensitive);
        }
    }
}
