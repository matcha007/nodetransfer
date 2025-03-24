package com.duansx.nodetransfer.client;

import cn.hutool.core.io.FileUtil;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;

public class DefaultFileStorage implements FileStorage {

    private static final Logger logger = LoggerFactory.getLogger(DefaultFileStorage.class);

    protected final File dir;

    public DefaultFileStorage(File dir) {
        this.dir = dir;
        if (!dir.exists()) {
            logger.debug("directory {} not exist,mkdir directory.", dir);
            dir.mkdirs();
        }
    }


    @Override
    public File getDir() {
        return dir;
    }

    @Override
    public File get(String fileName) {
        if (FileUtil.isAbsolutePath(fileName)) {
            return new File(fileName);
        } else {
            return new File(dir, fileName);
        }
    }


    @Override
    public InputStream getInputStream(File file) throws IOException {
        return getInputStream(file, null);
    }


    @Override
    public void save(File file, InputStream inputStream) throws IOException {
        save(file, inputStream, null);
    }

    @Override
    public byte[] read(File file, long offset, long chunkSize) throws IOException {
        return read(file, offset, chunkSize, null);
    }

    @Override
    public void write(File file, long offset, byte[] body) throws IOException {
        write(file, offset, body, null);
    }

    @Override
    public InputStream getInputStream(File file, Boolean doEncode) throws IOException {
        return Files.newInputStream(file.toPath());
    }

    @Override
    public void save(File file, InputStream inputStream, Boolean doEncode) throws IOException {
        try (OutputStream outputStream = Files.newOutputStream(file.toPath())) {
            IOUtils.copyLarge(inputStream, outputStream, new byte[1024 * 10]);
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }

    @Override
    public byte[] read(File file, long offset, long chunkSize, Boolean doEncode) throws IOException {
        // 设置缓冲区大小
        chunkSize = (int) Math.min(chunkSize, file.length() - offset);
        byte[] bytes = new byte[(int) chunkSize];
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file.getAbsoluteFile(), "r")) {
            //从游标开始读取
            randomAccessFile.seek(offset);
            randomAccessFile.read(bytes);
        }

        return bytes;
    }

    @Override
    public void write(File file, long offset, byte[] body, Boolean doEncode) throws IOException {
        File parentFile = file.getParentFile();
        if (!parentFile.exists()) {
            parentFile.mkdirs();
        }

        byte[] bytes = body != null ? body : new byte[0];
        try (RandomAccessFile raf = new RandomAccessFile(file.getAbsoluteFile(), "rw")) {
            raf.seek(offset);
            raf.write(bytes);
        }
    }
}
