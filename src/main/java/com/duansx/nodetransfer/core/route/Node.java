package com.duansx.nodetransfer.core.route;

import java.util.Objects;
import java.util.StringJoiner;

public class Node {

    private String name;

    private String addr;

    private boolean isWritable;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddr() {
        return addr;
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }

    public boolean isWritable() {
        return isWritable;
    }

    public void setWritable(boolean writable) {
        isWritable = writable;
    }

    public Node(String name) {
        this.name = name;
    }

    public Node(String name, String addr) {
        this.name = name;
        this.addr = addr;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Node node = (Node) o;
        return isWritable == node.isWritable && Objects.equals(name, node.name) && Objects.equals(addr, node.addr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, addr, isWritable);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", Node.class.getSimpleName() + "[", "]").add("name='" + name + "'").add("addr='" + addr + "'").add("isWritable=" + isWritable).toString();
    }
}
