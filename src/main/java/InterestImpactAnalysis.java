package com.lab2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class InterestImpactAnalysis {

    // 利率区间划分
    public static String getRateRange(double rate) {
        if (rate < 3) return "0%-3%";
        else if (rate < 4) return "3%-4%";
        else if (rate < 5) return "4%-5%";
        else if (rate < 6) return "5%-6%";
        else return "6%-7%";
    }

    // Mapper类
    public static class InterestImpactMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Map<String, Double> interestRateMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 获取利率文件的路径
            Path rateFilePath = new Path(context.getConfiguration().get("rateFilePath"));
            FileSystem fs = FileSystem.get(context.getConfiguration());
            
            // 读取文件并将 mfd_date 和 Interest_2_W 对应的数据存入 interestRateMap
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(rateFilePath)));
            String line;
            br.readLine(); // 跳过标题行
            
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",");
                if (fields.length >= 3) {
                    String date = fields[0]; // mfd_date
                    Double interest2W = Double.parseDouble(fields[3]); // Interest_2_W
                    interestRateMap.put(date, interest2W);
                }
            }
            br.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t");
            if (parts.length != 2) return;

            String date = parts[0];
            String[] inflowOutflow = parts[1].split(",");
            if (inflowOutflow.length != 2) return;

            long inflow = Long.parseLong(inflowOutflow[0]);
            long outflow = Long.parseLong(inflowOutflow[1]);

            Double rate = interestRateMap.get(date);  // 获取对应日期的2周利率
            if (rate != null) {
                String range = getRateRange(rate);  // 利率区间
                context.write(new Text(range), new Text(inflow + "," + outflow + ",1"));
            }
        }
    }

    // Reducer类
    public static class InterestImpactReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long totalInflow = 0;
            long totalOutflow = 0;
            int days = 0;

            for (Text value : values) {
                String[] parts = value.toString().split(",");
                totalInflow += Long.parseLong(parts[0]);
                totalOutflow += Long.parseLong(parts[1]);
                days += Integer.parseInt(parts[2]);
            }

            long avgInflow = totalInflow / days;
            long avgOutflow = totalOutflow / days;

            context.write(key, new Text(avgInflow + "," + avgOutflow));
        }
    }

    // Driver类
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: InterestImpactAnalysis <balance_input_path> <rate_input_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("rateFilePath", args[1]);  // 设置利率文件路径
        Job job = Job.getInstance(conf, "Interest Impact Analysis");
        job.setJarByClass(InterestImpactAnalysis.class);

        job.setMapperClass(InterestImpactMapper.class);
        job.setReducerClass(InterestImpactReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));  // 资金流输入
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
