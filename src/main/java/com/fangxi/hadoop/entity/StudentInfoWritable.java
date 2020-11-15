package com.fangxi.hadoop.entity;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StudentInfoWritable implements WritableComparable<StudentInfoWritable> {
    private String name;
    private Integer score;

    public StudentInfoWritable() {
    }

    public StudentInfoWritable(String name, Integer score) {
        this.name = name;
        this.score = score;
    }

    @Override
    public String toString() {
        return name + " " + score;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getScore() {
        return score;
    }

    public void setScore(Integer score) {
        this.score = score;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(score);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.name = in.readUTF();
        this.score = in.readInt();
    }

    @Override
    public int compareTo(StudentInfoWritable o) {
        //降序
        if (this.getScore() < o.getScore()) {
            return 1;
        } else if (this.getScore().equals(o.getScore())) {
            return 0;
        } else {
            return -1;
        }
    }
}
