package com.atguigu.mr;

import com.atguigu.util.ETLUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class ETLMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,NullWritable,Text> {

    //定义全局的v
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取数据
        String oriStr = value.toString();

        //过滤数据
        String etlStr = ETLUtil.etlStr(oriStr);

        //写出
        if(etlStr==null){
            return;

        }
        v.set(etlStr);
        context.write(NullWritable.get(), v);
    }
}
