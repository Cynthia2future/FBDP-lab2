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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class WeekdayCount {

    // Mapper类
    public static class WeekdayCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        private final String[] daysOfWeek = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");

            if (fields.length == 2) {
                String reportDate = fields[0];  // 日期
                String[] amounts = fields[1].split(",");
                long inflow = Long.parseLong(amounts[0]);  // 资金流入量
                long outflow = Long.parseLong(amounts[1]); // 资金流出量

                try {
                    Date date = dateFormat.parse(reportDate);
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTime(date);
                    String weekday = daysOfWeek[calendar.get(Calendar.DAY_OF_WEEK) - 1];

                    // 输出key为星期几，value为<资金流入量,资金流出量>的组合
                    context.write(new Text(weekday + "_inflow"), new LongWritable(inflow));
                    context.write(new Text(weekday + "_outflow"), new LongWritable(outflow));
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // Reducer类
    public static class WeekdayCountReducer extends Reducer<Text, LongWritable, Text, Text> {

        private final Map<String, Long> inflowSums = new HashMap<>();
        private final Map<String, Long> outflowSums = new HashMap<>();
        private final Map<String, Integer> counts = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            String[] keyParts = key.toString().split("_");
            String weekday = keyParts[0];
            String type = keyParts[1];

            long sum = 0;
            int count = 0;

            for (LongWritable value : values) {
                sum += value.get();
                count++;
            }

            if (type.equals("inflow")) {
                inflowSums.put(weekday, inflowSums.getOrDefault(weekday, 0L) + sum);
            } else {
                outflowSums.put(weekday, outflowSums.getOrDefault(weekday, 0L) + sum);
            }
            counts.put(weekday, counts.getOrDefault(weekday, 0) + count);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Map<String, Long> inflowAverages = new HashMap<>();
            Map<String, Long> outflowAverages = new HashMap<>();

            for (String weekday : inflowSums.keySet()) {
                int count = counts.getOrDefault(weekday, 1);
                long inflowAverage = inflowSums.getOrDefault(weekday, 0L) / count;
                long outflowAverage = outflowSums.getOrDefault(weekday, 0L) / count;
                inflowAverages.put(weekday, inflowAverage);
                outflowAverages.put(weekday, outflowAverage);
            }

            // 按照资金流入量从大到小排序
            inflowAverages.entrySet().stream()
                    .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                    .forEach(entry -> {
                        String weekday = entry.getKey();
                        try {
                            context.write(new Text(weekday), new Text(inflowAverages.get(weekday) + "," + outflowAverages.get(weekday)));
                        } catch (IOException | InterruptedException e) {
                            e.printStackTrace();
                        }
                    });
        }
    }

    // Driver类
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WeekdayCount <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WeekdayCount");
        job.setJarByClass(WeekdayCount.class);

        job.setMapperClass(WeekdayCountMapper.class);
        job.setReducerClass(WeekdayCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
