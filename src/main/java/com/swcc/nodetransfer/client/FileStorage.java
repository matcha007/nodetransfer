package com.swcc.nodetransfer.client;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public interface FileStorage {

    File getDir();

    File get(String fileName);

    InputStream getInputStream(File file) throws IOException;

    void save(File file, InputStream inputStream) throws IOException;

    byte[] read(File file, long offset, long chunkSize) throws IOException;

    void write(File file, long offset, byte[] body) throws IOException;

    InputStream getInputStream(File file, Boolean doEncode) throws IOException;

    void save(File file, InputStream inputStream, Boolean doEncode) throws IOException;

    byte[] read(File file, long offset, long chunkSize, Boolean doEncode) throws IOException;

    void write(File file, long offset, byte[] body, Boolean doEncode) throws IOException;

}