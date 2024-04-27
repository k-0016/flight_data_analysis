package com.example.flightdata;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlightDataAnalysis {

    // Mapper for On-time Performance
    public static class OnTimePerformanceMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final int DELAY_THRESHOLD = 10; // Delay threshold in minutes

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            try {
                String carrier = fields[8];
                int arrDelay = Integer.parseInt(fields[14]);
                int depDelay = Integer.parseInt(fields[15]);
                int totalDelay = arrDelay + depDelay;

                context.write(new Text(carrier), new IntWritable(totalDelay <= DELAY_THRESHOLD ? 1 : 0));
            } catch (NumberFormatException e) {
                // Handle parsing errors
            }
        }
    }

    // Reducer for On-time Performance
    public static class OnTimePerformanceReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int totalFlights = 0;
            int onTimeFlights = 0;

            for (IntWritable val : values) {
                totalFlights++;
                if (val.get() == 1) {
                    onTimeFlights++;
                }
            }

            double onTimePercentage = 100.0 * onTimeFlights / totalFlights;
            context.write(key, new DoubleWritable(onTimePercentage));
        }
    }

    // Mapper for Average Taxi Time
    public static class TaxiTimeMapper extends Mapper<Object, Text, Text, IntWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            try {
                String origin = fields[16];
                int taxiOutTime = Integer.parseInt(fields[20]);
                String destination = fields[17];
                int taxiInTime = Integer.parseInt(fields[19]);

                context.write(new Text(origin), new IntWritable(taxiOutTime));
                context.write(new Text(destination), new IntWritable(taxiInTime));
            } catch (NumberFormatException e) {
                // Handle parsing errors
            }
        }
    }

    // Reducer for Average Taxi Time
    public static class TaxiTimeReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int totalTaxiTime = 0;
            int count = 0;

            for (IntWritable val : values) {
                totalTaxiTime += val.get();
                count++;
            }

            double averageTaxiTime = (double) totalTaxiTime / count;
            context.write(key, new DoubleWritable(averageTaxiTime));
        }
    }

    // Main method to set up and run the jobs
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: FlightDataAnalysis <input path> <output path for On-Time> <output path for Taxi Time> <output path for Cancellation>");
            System.exit(-1);
        }

        // Setup and run the On-Time Performance job
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "On-Time Performance");
        job1.setJarByClass(FlightDataAnalysis.class);
        job1.setMapperClass(OnTimePerformanceMapper.class);
        job1.setReducerClass(OnTimePerformanceReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Setup and run the Average Taxi Time job
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Average Taxi Time");
        job2.setJarByClass(FlightDataAnalysis.class);
        job2.setMapperClass(TaxiTimeMapper.class);
        job2.setReducerClass(TaxiTimeReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }
    }
}

