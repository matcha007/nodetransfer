package com.swcc.nodetransfer.client;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

import java.io.File;

public class FileHeader implements CommandCustomHeader {

    private String localFilePath;

    private Boolean localDoEncode;

    private String remoteFilePath;

    private Boolean remoteDoEncode;

    private long fileSize;

    private long chunkSize;

    private long offset;

    public FileHeader() {
    }

    public FileHeader(File file) {
        this(file, 1024 * 1024);
    }

    public FileHeader(File file, int chunkSize) {
        this(file, 0L, chunkSize);
    }

    public FileHeader(File file, long offset, int chunkSize) {
        if (file == null) {
            throw new NullPointerException("file");
        } else if (offset < 0L) {
            throw new IllegalArgumentException("offset: " + offset + " (expected: 0 or greater)");
        } else if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize: " + chunkSize + " (expected: a positive integer)");
        } else {
            this.localFilePath = file.getAbsolutePath();
            this.remoteFilePath = file.getAbsolutePath();
            this.fileSize = file.length();
            this.offset = offset;
            this.chunkSize = chunkSize;
        }
    }


    public String getLocalFilePath() {
        return localFilePath;
    }

    public void setLocalFilePath(String localFilePath) {
        this.localFilePath = localFilePath;
    }

    public String getRemoteFilePath() {
        return remoteFilePath;
    }

    public void setRemoteFilePath(String remoteFilePath) {
        this.remoteFilePath = remoteFilePath;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public long getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(long chunkSize) {
        this.chunkSize = chunkSize;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public Boolean getLocalDoEncode() {
        return localDoEncode;
    }

    public void setLocalDoEncode(Boolean localDoEncode) {
        this.localDoEncode = localDoEncode;
    }

    public Boolean getRemoteDoEncode() {
        return remoteDoEncode;
    }

    public void setRemoteDoEncode(Boolean remoteDoEncode) {
        this.remoteDoEncode = remoteDoEncode;
    }

    @Override
    public void checkFields() throws RemotingCommandException {

    }
}
