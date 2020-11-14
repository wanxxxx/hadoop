package com.fangxi.hadoop.entity;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AreaIfoWritable implements Writable {
    private Integer id = 0;
    private String area = "";
    private Integer year = 0;
    private Integer count = 0;


    public AreaIfoWritable() {
    }

    public AreaIfoWritable(Integer id, String area, Integer year, Integer count) {
        this.id = id;
        this.area = area;
        this.year = year;
        this.count = count;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(area);
        out.writeInt(year);
        out.writeInt(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.id = in.readInt();
        this.area = in.readUTF();
        this.year = in.readInt();
        this.count = in.readInt();
    }

    public void changeTo(AreaIfoWritable o) {
        this.setId(o.getId());
        this.setArea(o.getArea());
        this.setYear(o.getYear());
        this.setCount(o.getCount());
    }

    @Override
    public String toString() {
        return id+" "+area+" "+year+" "+count;
    }
}
