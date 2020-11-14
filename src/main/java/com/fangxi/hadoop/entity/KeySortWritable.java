package com.fangxi.hadoop.entity;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KeySortWritable implements WritableComparable<KeySortWritable> {
    private Integer outkey = 0;

    public Integer getOutkey() {
        return outkey;
    }

    public void setOutkey(Integer outkey) {
        this.outkey = outkey;
    }

    @Override
    public String toString() {
        return "KeySortWritable{" +
                "outkey=" + outkey +
                '}';
    }

    @Override
    public int compareTo(KeySortWritable o) {
        return -ascSort(o);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(outkey);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.outkey = in.readInt();
    }

    private int ascSort(KeySortWritable o) {
        if (this.getOutkey() < o.getOutkey()) {
            return -1;
        }else if (this.getOutkey() < o.getOutkey()) {
            return 0;
        }
        else{
            return 1;
        }
    }
}
