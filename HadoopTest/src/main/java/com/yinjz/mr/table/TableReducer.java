package com.yinjz.mr.table;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text,TableBean,TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        ArrayList<TableBean> orderBeans = new ArrayList<>();

        TableBean pdBean = new TableBean();

        for (TableBean tableBean: values) {
            if ("order".equals(tableBean.getFlag())) {
                TableBean tmpTable = new TableBean();

                try {
                    BeanUtils.copyProperties(tmpTable,tableBean);
                    orderBeans.add(tmpTable);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    BeanUtils.copyProperties(pdBean,tableBean);
                    orderBeans.add(pdBean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }

            }

        }
        for (TableBean tableBean : orderBeans) {
            tableBean.setPname(pdBean.getPname());
            context.write(tableBean, NullWritable.get());
        }
    }
}
