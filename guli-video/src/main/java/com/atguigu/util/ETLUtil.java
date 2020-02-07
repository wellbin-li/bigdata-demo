package com.atguigu.util;

public class ETLUtil {

    /**
     * 1.过滤长度不够的，小于9个字段
     * 2.去掉类别字段中的空格
     * 3.修改相关视频ID字段的分隔符，由‘\t’的替换为‘&’
     *
     * @param oriStr 输入参数，原始数据
     * @return 过滤后的数据
     */
    public static String etlStr(String oriStr) {

        StringBuffer sb = new StringBuffer();

        //1.切割
        String[] fields = oriStr.split("\t");

        //2.对字段长度进行过滤
        if(fields.length<9){
            return null;
        }

        //3.去掉类别字段中的空格
        fields[3] = fields[3].replaceAll(" ", "");

        //4.修改相关视频ID字段d的分隔符，由‘\t’的替换为‘&’
        for (int i = 0; i < fields.length; i++) {
            //对非相关ID字段进行处理
            if(i<9){
                if(i==fields.length-1){
                    sb.append(fields[i]);
                }else{
                    sb.append(fields[i]).append("\t");
                }
            }else{
                //对相关ID字段进行处理
                if(i==fields.length-1){
                    sb.append(fields[i]);
                }else{
                    sb.append(fields[i]).append("&");
                }
            }
        }

        //5.返回结果

        return sb.toString();
    }

    public static void main(String[] args) {
        System.out.println(ETLUtil.etlStr("LKh7zAJ4nwo\tTheReceptionist\t653\tEntertainment\t424\t13021\t4.34\t1305\t744" +
                "\tDjdA-5oKYFQ\tNxTDlnOuybo\tc-8VuICzXtU\tDH56yrIO5nI\tW1Uo5DQTtzc\tE-3zXq_r4w0\t1TCeoRPg5dE\tyAr" +
                "26YhuYNY\t2ZgXx72XmoE\t-7ClGo-YgZ0\tvmdPOOd6cxI\tKRHfMQqSHpk\tpIMpORZthYw\t1tUDzOp10pk\theqocRij5P0\t" +
                "_XIuvoH6rUg\tLGVU5DsezE0\tuO2kj6_D8B4\txiDqywcDQRM\tuX81lMev6_o\n"));
    }
}
