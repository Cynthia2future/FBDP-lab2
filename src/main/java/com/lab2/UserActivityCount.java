package com.lab2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class UserActivityCount {

    // Mapper类
    public static class ActivityMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) {
                return;  // 跳过CSV文件头
            }

            String line = value.toString();
            String[] fields = line.split(",");

            if (fields.length >= 10) {  // 确保有足够的列
                String userId = fields[0];  // 用户ID
                String directPurchaseAmt = fields[5];  // 今日直接购买量
                String totalRedeemAmt = fields[9];     // 今日总赎回量

                // 如果有直接购买或赎回行为，标记为活跃
                if ((!directPurchaseAmt.isEmpty() && Long.parseLong(directPurchaseAmt) > 0) ||
                    (!totalRedeemAmt.isEmpty() && Long.parseLong(totalRedeemAmt) > 0)) {
                    context.write(new Text(userId), one);
                }
            }
        }
    }

    // Reducer类
    public static class ActivityReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int activeDays = 0;
            for (IntWritable value : values) {
                activeDays += value.get();
            }
            context.write(key, new IntWritable(activeDays));
        }
    }

    // Driver类
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: UserActivityCount <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "User Activity Count");
        job.setJarByClass(UserActivityCount.class);

        job.setMapperClass(ActivityMapper.class);
        job.setReducerClass(ActivityReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
