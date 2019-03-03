/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package playground.filesystem;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

public class FileSystemMock {

    public final FileSystem fileSystem;

    private String loremIpsum =
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Praesent auctor imperdiet " +
        "velit, eu dapibus nisl dapibus vitae. Sed quam lacus, fringilla posuere ligula at, " +
        "aliquet laoreet nulla. Aliquam id fermentum justo. Aliquam et massa consequat, " +
        "pellentesque dolor nec, gravida libero. Phasellus elit eros, finibus eget " +
        "sollicitudin ac, consectetur sed ante. Etiam ornare lacus blandit nisi gravida " +
        "accumsan. Sed in lorem arcu. Vivamus et eleifend ligula. Maecenas ut commodo ante. " +
        "Suspendisse sit amet placerat arcu, porttitor sagittis velit. Quisque gravida mi a " +
        "porttitor ornare. Cras lorem nisl, sollicitudin vitae odio at, vehicula maximus " +
        "mauris. Sed ac purus ac turpis pellentesque cursus ac eget est. Pellentesque " +
        "habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas.";

    public FileSystemMock(FileSystem fs) {
        fileSystem = fs;
    }

    public FileSystemMock() {
        this(Jimfs.newFileSystem(Configuration.unix()));
    }

    public void cleanFiles() {
        for (Path rootDir : getFileSystem().getRootDirectories()) {
            try {
                Files.walkFileTree(rootDir, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        Files.deleteIfExists(file);
                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    public void generateFiles(int numFiles, int pageSize, String basePath) {
        String base = "";
        if (!basePath.isEmpty()) {
            if ('/' != basePath.charAt(0)) {
                base = "/" + basePath;
            } else {
                base = basePath;
            }
        }
        int i = 1;
        while (i <= numFiles) {
            int j = i / pageSize;
            String subDir = (j > 0) ? "/dir_" + j : "";
            putFileOnFtp(base + subDir, "sample_" + i);
            i++;
        }
    }

    public void putFileOnFtp(String path, String fileName) {
        putFileOnFtpWithContents(path, fileName, loremIpsum.getBytes());
    }

    public void putFileOnFtpWithContents(String path, String fileName, byte[] fileContents) {
        try {
            Path baseDir = getFileSystem().getPath(path);
            if (!Files.exists(baseDir)) {
                Files.createDirectories(baseDir);
            }
            Path filePath = baseDir.resolve(fileName);
            Files.write(filePath, fileContents);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    FileSystem getFileSystem() {
        return fileSystem;
    }

    public String getLoremIpsum() {
        return loremIpsum;
    }

}
