package com.fangxi.hadoop.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
/*自定义Writable类*/
public class MedicineWritable implements WritableComparable<MedicineWritable> {
    private String date = "";
    private String id = "";
    private String num = "";
    private String name = "";
    private Double saleCount = 0D;
    private Double value1 = 0D;
    private Double value2 = 0D;

    public MedicineWritable() {
    }

    public MedicineWritable(String date, String id, String num, String name, Double saleCount, Double value1, Double value2) {
        this.date = date;
        this.id = id;
        this.num = num;
        this.name = name;
        this.saleCount = saleCount;
        this.value1 = value1;
        this.value2 = value2;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getNum() {
        return num;
    }

    public void setNum(String num) {
        this.num = num;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getSaleCount() {
        return saleCount;
    }

    public void setSaleCount(Double saleCount) {
        this.saleCount = saleCount;
    }

    public Double getValue1() {
        return value1;
    }

    public void setValue1(Double value1) {
        this.value1 = value1;
    }

    public Double getValue2() {
        return value2;
    }

    public void setValue2(Double value2) {
        this.value2 = value2;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.date = in.readUTF();
        this.id = in.readUTF();
        this.num = in.readUTF();
        this.name = in.readUTF();
        this.saleCount = in.readDouble();
        this.value1 = in.readDouble();
        this.value2 = in.readDouble();
    }

    @Override
    public String toString() {
        return date + "\t" + id + "\t" + num + "\t" + name +
                "\t" + saleCount + "\t" + value1 + "\t" + value2;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.date);
        out.writeUTF(this.id);
        out.writeUTF(this.num);
        out.writeUTF(this.name);
        out.writeDouble(this.saleCount);
        out.writeDouble(this.value1);
        out.writeDouble(this.value2);
    }


    public int compareTo(MedicineWritable o) {
        // 按实收金额降序
        return o.getValue2().compareTo(this.getValue2());
    }

    public void changeTo(MedicineWritable o) {
        this.setDate(o.getDate());
        this.setId(o.getId());
        this.setNum(o.getNum());
        this.setName(o.getName());
        this.setSaleCount(o.getSaleCount());
        this.setValue1(o.getValue1());
        this.setValue2(o.getValue2());
    }
}

