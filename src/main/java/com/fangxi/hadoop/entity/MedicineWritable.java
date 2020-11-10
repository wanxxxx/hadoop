package com.fangxi.hadoop.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class MedicineWritable implements WritableComparable<MedicineWritable> {
    private String date;
    private int id;
    private int num;
    private String name = null;
    private int saleNum;
    private Double value1 = 0D;
    private Double value2 = 0D;

    public MedicineWritable() {
    }

    public MedicineWritable(String date, int id, int num, String name, int saleNum, double value1, double value2) {
        this.date = date;
        this.id = id;
        this.num = num;
        this.name = name;
        this.saleNum = saleNum;
        this.value1 = value1;
        this.value2 = value2;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getSaleNum() {
        return saleNum;
    }

    public void setSaleNum(int saleNum) {
        this.saleNum = saleNum;
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
        this.date=in.readUTF();
        this.id=in.readInt();
        this.num=in.readInt();
        this.name=in.readUTF();
        this.saleNum=in.readInt();
        this.value1=in.readDouble();
        this.value2=in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.date);
        out.writeInt(this.id);
        out.writeInt(this.num);
        out.writeUTF(this.name);
        out.writeInt(this.saleNum);
        out.writeDouble(this.value1);
        out.writeDouble(this.value2);
    }

    public int compareTo(MedicineWritable o) {
        // 按实收金额降序
        return o.getValue2().compareTo(this.getValue2());
    }

}

