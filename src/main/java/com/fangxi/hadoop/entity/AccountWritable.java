package com.fangxi.hadoop.entity;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AccountWritable implements WritableComparable<AccountWritable> {
    private String name;
    private Integer cost;

    public AccountWritable() {
    }

    public AccountWritable(String name, Integer cost) {
        this.name = name;
        this.cost = cost;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getCost() {
        return cost;
    }

    public void setCost(Integer cost) {
        this.cost = cost;
    }

    @Override
    public String toString() {
        return name + "  " + cost;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(cost);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.name = in.readUTF();
        this.cost = in.readInt();
    }

    @Override
    public int compareTo(AccountWritable o) {
        //第一次排序根据用户名进行正序排序
        return o.getName().compareTo(o.getName());
    }
}
