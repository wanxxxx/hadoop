package com.fangxi.hadoop.util;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DescSortComparator extends WritableComparator{
    public DescSortComparator() {
        super(IntWritable.class,true);
    }
    /**
     * TODO(自定义比较器)
     * @author 方希
     * @Date 2019年6月17日
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        IntWritable wa = (IntWritable) a;
        IntWritable wb = (IntWritable) b;
        //倒序比较
        return wb.compareTo(wa);
    }

}
