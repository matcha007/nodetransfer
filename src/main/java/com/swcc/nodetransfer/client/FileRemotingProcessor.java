package com.swcc.nodetransfer.client;

import com.alibaba.fastjson.JSONObject;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class FileRemotingProcessor implements NettyRequestProcessor {

    private static final Logger logger = LoggerFactory.getLogger(FileRemotingProcessor.class);

    private final FileStorage fileStorage;

    public FileRemotingProcessor(FileStorage fileStorage) {
        this.fileStorage = fileStorage;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        FileHeader fileHeader = (FileHeader) request.decodeCommandCustomHeader(FileHeader.class);
        RemotingCommand response = RemotingCommand.createResponseCommand(FileHeader.class);
        response.setOpaque(request.getOpaque());
        response.writeCustomHeader(fileHeader);

        byte[] bytes = null;
        if (2 == request.getCode()) {
            bytes = upload(fileHeader, request.getBody());
        } else {
            bytes = download(fileHeader);
        }

        response.setBody(bytes);
        response.setCode(0);
        return response;
    }


    protected byte[] download(FileHeader fileHeader) throws IOException {
        File dstFile = fileStorage.get(fileHeader.getRemoteFilePath());
        fileHeader.setFileSize(dstFile.length());
        return fileStorage.read(dstFile, fileHeader.getOffset(), fileHeader.getChunkSize(), fileHeader.getRemoteDoEncode());
    }

    protected byte[] upload(FileHeader fileHeader, byte[] body) throws IOException {
        File dstFile = fileStorage.get(fileHeader.getRemoteFilePath());
        String absolutePath = dstFile.getAbsolutePath();
        if (dstFile.exists()) {
            logger.warn("fileName[{}]--already exist", absolutePath);
            fileHeader.setOffset(dstFile.length());
            return JSONObject.toJSONBytes(absolutePath);
        }

        File tempFile = new File(absolutePath + ".tmp");
        if (tempFile.length() == fileHeader.getOffset()) {
            fileStorage.write(tempFile, fileHeader.getOffset(), body, fileHeader.getRemoteDoEncode());
        }
        fileHeader.setOffset(tempFile.length());

        if (tempFile.length() >= fileHeader.getFileSize()) {
            if (!tempFile.renameTo(dstFile)) {
                Files.copy(tempFile.toPath(), dstFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                tempFile.delete();
            }
            return JSONObject.toJSONBytes(absolutePath);
        }

        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

}
