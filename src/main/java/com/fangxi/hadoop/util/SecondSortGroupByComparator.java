package com.fangxi.hadoop.util;

import com.fangxi.hadoop.entity.AccountWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondSortGroupByComparator extends WritableComparator {
    public SecondSortGroupByComparator() {
        super(AccountWritable.class, true);
    }

    /**
     * TODO(自定义比较器)
     *
     * @author 方希
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        AccountWritable wa = (AccountWritable) a;
        AccountWritable wb = (AccountWritable) b;
        //用户名倒序排序
        return wa.getName().compareTo(wb.getName());

    }

}
