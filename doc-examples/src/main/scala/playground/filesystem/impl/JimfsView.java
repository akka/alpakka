/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package playground.filesystem.impl;

import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * File system view based on the in-memory jimfs file system. The root in this
 * class is the user virtual root (/).
 */
public class JimfsView implements FileSystemView {

    private final Logger LOG = LoggerFactory.getLogger(JimfsView.class);

    // this will be the jimfs file system in runtime.
    private FileSystem fileSystem;

    // the root directory will always end with '/'.
    private String rootDir;

    // the first and the last character will always be '/'
    // It is always with respect to the root directory.
    private String currDir;

    private User user;

    private boolean caseInsensitive = false;

    public JimfsView(FileSystem fileSystem, User user, boolean caseInsensitive)
        throws FtpException {
        if (fileSystem == null) {
            throw new IllegalArgumentException("filesystem can not be null");
        }
        if (user == null) {
            throw new IllegalArgumentException("user can not be null");
        }
        if (user.getHomeDirectory() == null) {
            throw new IllegalArgumentException("user home directory can not be null");
        }

        this.fileSystem = fileSystem;

        this.caseInsensitive = caseInsensitive;

        // add last '/' if necessary
        String rootDir = user.getHomeDirectory();
        rootDir = normalizeSeparateChar(rootDir);
        if (!rootDir.endsWith("/")) {
            rootDir += '/';
        }

        LOG.debug("Jimfs filesystem view created by user \"{}\" with root \"{}\"", user.getName(), rootDir);

        this.rootDir = rootDir;

        this.user = user;

        currDir = "/";

    }

    /**
     * Get the user home directory. It would be the file system root
     * for the specific user.
     */
    public FtpFile getHomeDirectory() throws FtpException {
        return new JimfsFtpFile("/", fileSystem.getPath(rootDir), user);
    }

    /**
     * Get the current directory.
     */
    public FtpFile getWorkingDirectory() throws FtpException {
        FtpFile fileObj;
        if (currDir.equals("/")) {
            fileObj = getHomeDirectory();
        } else {
            Path path = fileSystem.getPath(rootDir, currDir.substring(1));
            fileObj = new JimfsFtpFile(currDir, path, user);
        }
        return fileObj;
    }

    /**
     * Get the file object.
     */
    public FtpFile getFile(String file) {
        String physicalName = getPhysicalName(file);
        Path filePath = fileSystem.getPath(physicalName);

        // strip the root directory and return
        String userFileName = physicalName.substring(rootDir.length() - 1);
        return new JimfsFtpFile(userFileName, filePath, user);
    }

    /**
     * Change directory.
     */
    public boolean changeWorkingDirectory(String dir) throws FtpException {

        // not a directory - return false
        dir = getPhysicalName(dir);
        Path dirPath = fileSystem.getPath(dir);
        if (!Files.isDirectory(dirPath)) {
            return false;
        }

        // strip user root and add last '/' if necessary
        dir = dir.substring(rootDir.length() - 1);
        if (dir.charAt(dir.length() - 1) != '/') {
            dir = dir + '/';
        }

        currDir = dir;
        return true;
    }

    /**
     * Is the file content random accessible?
     */
    public boolean isRandomAccessible() {
        return true;
    }

    /**
     * Dispose the file system.
     */
    public void dispose() {
        // Nothing to do
    }

    private String normalizeSeparateChar(final String pathName) {
        String normalizePathName = pathName.replace(fileSystem.getSeparator(), "/");
        return normalizePathName.replace('\\', '/');
    }

    private String getPhysicalName(final String file) {

        // get the starting directory
        String normalizedRootDir = normalizeSeparateChar(rootDir);
        if (normalizedRootDir.charAt(normalizedRootDir.length() - 1) != '/') {
            normalizedRootDir += '/';
        }

        String normalizedFileName = normalizeSeparateChar(file);
        String resArg;
        String normalizedCurrDir = currDir;
        if (normalizedFileName.charAt(0) != '/') {
            if (normalizedCurrDir == null || normalizedCurrDir.length() == 0) {
                normalizedCurrDir = "/";
            }

            normalizedCurrDir = normalizeSeparateChar(normalizedCurrDir);

            if (normalizedCurrDir.charAt(0) != '/') {
                normalizedCurrDir = '/' + normalizedCurrDir;
            }
            if (normalizedCurrDir.charAt(normalizedCurrDir.length() - 1) != '/') {
                normalizedCurrDir += '/';
            }

            resArg = normalizedRootDir + normalizedCurrDir.substring(1);
        } else {
            resArg = normalizedRootDir;
        }

        // strip last '/'
        if (resArg.charAt(resArg.length() - 1) == '/') {
            resArg = resArg.substring(0, resArg.length() - 1);
        }

        // replace ., ~ and ..
        // in this loop resArg will never end with '/'
        StringTokenizer st = new StringTokenizer(normalizedFileName, "/");
        while (st.hasMoreTokens()) {
            String tok = st.nextToken();

            // . => current directory
            if (tok.equals(".")) {
                continue;
            }

            // .. => parent directory (if not root)
            if (tok.equals("..")) {
                if (resArg.startsWith(normalizedRootDir)) {
                    int slashIndex = resArg.lastIndexOf("/");
                    if (slashIndex != -1) {
                        resArg = resArg.substring(0, slashIndex);
                    }
                }
                continue;
            }

            // ~ => home directory (in this case is the root directory)
            if (tok.equals("~")) {
                resArg = normalizedRootDir.substring(0, normalizedRootDir.length() - 1);
                continue;
            }

            if (caseInsensitive) {
                Path dir = fileSystem.getPath(resArg);
                DirectoryStream<Path> dirStream = null;
                try {
                    dirStream = Files.newDirectoryStream(dir, new NameEqualsPathFilter(tok, true));
                } catch (IOException t) {
                    // ignore
                }
                List<Path> matches = new ArrayList<>(0);
                if (dirStream != null) {
                    for (Path match : dirStream) {
                        matches.add(match);
                    }
                }
                if (matches.size() > 0) {
                    tok = matches.get(0).getFileName().toString();
                }
            }

            resArg = resArg + '/' + tok;
        }

        // add last slash if necessary
        if ((resArg.length()) + 1 == normalizedRootDir.length()) {
            resArg += '/';
        }

        // final check
        if (!resArg.regionMatches(0, normalizedRootDir, 0, normalizedRootDir
                .length())) {
            resArg = normalizedRootDir;
        }

        return resArg;
    }
}
