package com.lab2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class BalanceCount {

    // Mapper类
    public static class BalanceCountMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 跳过CSV文件的头行
            if (key.get() == 0) {
                return;
            }

            String line = value.toString();
            String[] fields = line.split(",");

            if (fields.length >= 11) {  // 确保有足够的列
                String reportDate = fields[1];  // 日期
                String totalPurchaseAmt = fields[4].isEmpty() ? "0" : fields[4];  // 资金流入量
                String totalRedeemAmt = fields[9].isEmpty() ? "0" : fields[9];    // 资金流出量

                // 输出 key: 日期，value: 资金流入量,资金流出量
                context.write(new Text(reportDate), new Text(totalPurchaseAmt + "," + totalRedeemAmt));
            }
        }
    }

    // Reducer类
    public static class BalanceCountReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long totalInflow = 0;
            long totalOutflow = 0;

            for (Text value : values) {
                String[] amounts = value.toString().split(",");
                long inflow = Long.parseLong(amounts[0]);
                long outflow = Long.parseLong(amounts[1]);

                totalInflow += inflow;
                totalOutflow += outflow;
            }

            // 输出格式: <日期> TAB <资金流入量>,<资金流出量>
            context.write(key, new Text(totalInflow + "," + totalOutflow));
        }
    }

    // Driver类
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: BalanceCount <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BalanceCount");
        job.setJarByClass(BalanceCount.class);

        job.setMapperClass(BalanceCountMapper.class);
        job.setReducerClass(BalanceCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
