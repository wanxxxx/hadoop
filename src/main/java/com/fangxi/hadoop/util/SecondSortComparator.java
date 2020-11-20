package com.fangxi.hadoop.util;

import com.fangxi.hadoop.entity.AccountWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondSortComparator extends WritableComparator{
    public SecondSortComparator() {
        super(AccountWritable.class,true);
    }
    /**
     * TODO(自定义比较器)
     * @author 方希
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        AccountWritable wa = (AccountWritable) a;
        AccountWritable wb = (AccountWritable) b;
        int result=wa.compareTo(wb);
        if(result==0){
            //用户名相等，倒序排序
            result=wb.getCost().compareTo(wa.getCost());
        }
        //倒序比较
        return result;
    }

}
