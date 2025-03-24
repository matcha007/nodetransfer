package com.swcc.nodetransfer.client;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class NodeTransferHeader implements CommandCustomHeader {

        private String targetNodeCode;
        private String targetBusCode;

        public NodeTransferHeader() {
        }

        public NodeTransferHeader(String targetNodeCode, String targetBusCode) {
            this.targetNodeCode = targetNodeCode;
            this.targetBusCode = targetBusCode;
        }

        public String getTargetNodeCode() {
            return targetNodeCode;
        }

        public void setTargetNodeCode(String targetNodeCode) {
            this.targetNodeCode = targetNodeCode;
        }

        public String getTargetBusCode() {
            return targetBusCode;
        }

        public void setTargetBusCode(String targetBusCode) {
            this.targetBusCode = targetBusCode;
        }

        @Override
        public void checkFields() throws RemotingCommandException {

        }
    }