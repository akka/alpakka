/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package playground.filesystem.impl;

import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.User;
import org.apache.ftpserver.usermanager.impl.WriteRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class JimfsFtpFile implements FtpFile {

    private final Logger LOG = LoggerFactory.getLogger(JimfsFtpFile.class);

    // the file name with respect to the user root (so it's the virtual filename).
    // The path separator character will be '/' and
    // it will always begin with '/'.
    private String fileName;

    // The `physical` path in the underlying file system (but happens that
    // in this case, the `physical` path will be also virtusl (jimfs), so crazy).
    private Path path;

    private User user;

    protected JimfsFtpFile(final String fileName, final Path path,
            final User user) {
        if (fileName == null) {
            throw new IllegalArgumentException("fileName can not be null");
        }
        if (path == null) {
            throw new IllegalArgumentException("path can not be null");
        }
        if (fileName.length() == 0) {
            throw new IllegalArgumentException("fileName can not be empty");
        } else if (fileName.charAt(0) != '/') {
            throw new IllegalArgumentException(
                    "fileName must be an absolute path");
        }

        this.fileName = fileName;
        this.path = path;
        this.user = user;
    }

    public String getAbsolutePath() {

        // strip the last '/' if necessary
        String fullName = fileName;
        int filelen = fullName.length();
        if ((filelen != 1) && (fullName.charAt(filelen - 1) == '/')) {
            fullName = fullName.substring(0, filelen - 1);
        }
        return fullName;
    }

    public String getName() {

        // root - the short name will be '/'
        if (fileName.equals("/")) {
            return "/";
        }

        // strip the last '/'
        String shortName = fileName;
        int filelen = fileName.length();
        if (shortName.charAt(filelen - 1) == '/') {
            shortName = shortName.substring(0, filelen - 1);
        }

        // return from the last '/'
        int slashIndex = shortName.lastIndexOf('/');
        if (slashIndex != -1) {
            shortName = shortName.substring(slashIndex + 1);
        }
        return shortName;
    }

    public boolean isHidden() {
        try {
            return Files.isHidden(path);
        } catch (IOException t) {
            LOG.error(t.getMessage());
        }
        return false;
    }

    public boolean isDirectory() {
        return Files.isDirectory(path);
    }

    public boolean isFile() {
        return Files.isRegularFile(path);
    }

    public boolean doesExist() {
        return Files.exists(path);
    }

    public long getSize() {
        try {
            return Files.size(path);
        } catch (IOException t) {
            LOG.error(t.getMessage());
        }
        return -1;
    }

    public String getOwnerName() {
        return "user";
    }

    public String getGroupName() {
        return "group";
    }

    public int getLinkCount() {
        return Files.isDirectory(path) ? 3 : 1;
    }

    public long getLastModified() {
        try {
            return Files.getLastModifiedTime(path).toMillis();
        } catch (IOException t) {
            LOG.error(t.getMessage());
        }
        return -1;
    }

    public boolean setLastModified(long time) {
        try {
            Files.setLastModifiedTime(path, FileTime.fromMillis(time));
            return true;
        } catch (IOException t) {
            LOG.error(t.getMessage());
        }
        return false;
    }

    public boolean isReadable() {
        return Files.isReadable(path);
    }

    public boolean isWritable() {
        LOG.debug("Checking authorization for " + getAbsolutePath());
        if (user.authorize(new WriteRequest(getAbsolutePath())) == null) {
            LOG.debug("Not authorized");
            return false;
        }

        LOG.debug("Checking if file exists");
        if (Files.exists(path)) {
            LOG.debug("Checking can write: " + getAbsolutePath());
            return Files.isWritable(path);
        }

        LOG.debug("Authorized");
        return true;
    }

    public boolean isRemovable() {

        // root cannot be deleted
        if ("/".equals(fileName)) {
            return false;
        }

        String fullName = getAbsolutePath();

        // we check FTPServer's write permission for this file.
        if (user.authorize(new WriteRequest(fullName)) == null) {
            return false;
        }

        // In order to maintain consistency, when possible we delete the last '/' character in the String
        int indexOfSlash = fullName.lastIndexOf('/');
        String parentFullName;
        if (indexOfSlash == 0) {
            parentFullName = "/";
        } else {
            parentFullName = fullName.substring(0, indexOfSlash);
        }

        JimfsFtpFile parentObject = new JimfsFtpFile(parentFullName,
                path.getParent(), user);
        return parentObject.isWritable();
    }

    public boolean delete() {
        boolean retVal = false;
        try {
            if (isRemovable()) {
                Files.delete(path);
                retVal = true;
            }
        } catch (IOException t) {
            LOG.error(t.getMessage());
        }
        return retVal;
    }

    public boolean move(final FtpFile dest) {
        boolean retVal = false;
        if (dest.isWritable() && isReadable()) {
            Path destPath = ((JimfsFtpFile) dest).path;

            if (Files.exists(destPath)) {
                // renameTo behaves differently on different platforms
                // this check verifies that if the destination already exists,
                // we fail
                retVal = false;
            } else {
                try {
                    Files.move(path, destPath, StandardCopyOption.REPLACE_EXISTING);
                    retVal = true;
                } catch (IOException t) {
                    LOG.error(t.getMessage());
                }
            }
        }
        return retVal;
    }

    public boolean mkdir() {
        boolean retVal = false;
        try {
            if (isWritable()) {
                Files.createDirectory(path);
                retVal = true;
            }
        } catch (IOException t) {
            LOG.error(t.getMessage());
        }
        return retVal;
    }

    public Path getPhysicalFile() {
        return path;
    }

    public List<FtpFile> listFiles() {

        // is a directory
        if (!Files.isDirectory(path)) {
            return null;
        }

        // directory - return all the files
        DirectoryStream<Path> filesStream = null;
        try {
            filesStream = Files.newDirectoryStream(path);
        } catch(IOException t) {
            LOG.error(t.getMessage());
        }

        if (filesStream == null) {
            return null;
        }

        List<Path> files = new ArrayList<>();
        for (Path path : filesStream) {
            files.add(path);
        }

        // make sure the files are returned in order
        Collections.sort(files, new Comparator<Path>() {
            public int compare(Path o1, Path o2) {
                return o1.getFileName().compareTo(o2.getFileName());
            }
        });

        // get the virtual name of the base directory
        final String virtualFileStr =
                getAbsolutePath().charAt(getAbsolutePath().length() - 1) != '/'
                        ? getAbsolutePath() + '/' : getAbsolutePath();

        // now return all the files under the directory
        List<FtpFile> virtualFiles = new ArrayList<>(files.size());
        for (Path file : files) {
            String fileName = virtualFileStr + file.getFileName();
            virtualFiles.add(new JimfsFtpFile(fileName, file, user));
        }
        return virtualFiles;
    }

    public OutputStream createOutputStream(long offset)
            throws IOException {

        // permission check
        if (!isWritable()) {
            throw new IOException("No write permission : " + path.getFileName());
        }

        // create output stream
        final RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw");
        raf.setLength(offset);
        raf.seek(offset);

        // The IBM jre needs to have both the stream and the random access file
        // objects closed to actually close the file
        return new FileOutputStream(raf.getFD()) {
            @Override
            public void close() throws IOException {
                super.close();
                raf.close();
            }
        };
    }

    public InputStream createInputStream(long offset)
            throws IOException {

        // permission check
        if (!isReadable()) {
            throw new IOException("No read permission : " + path.getFileName());
        }

        return Files.newInputStream(path, StandardOpenOption.READ);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof JimfsFtpFile) {
            Path otherPath = ((JimfsFtpFile) obj).path.normalize();
            return this.path.normalize().equals(otherPath);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return path.normalize().hashCode();
    }
}
