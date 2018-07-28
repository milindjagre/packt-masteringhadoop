package packt.temperature;

/**
 * Maximum Temperature using Apache MapReduce
 * Owner: packt publishing
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaximumTemperature {

	public static class TemperatureMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
		public static final int MISSING = 9999;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			int year = Integer.parseInt(line.substring(15, 19));
			int temperature;
			if (line.charAt(87) == '+')
				temperature = Integer.parseInt(line.substring(88, 92));
			else
				temperature = Integer.parseInt(line.substring(87, 92));
			String quality = line.substring(92, 93);
			if (temperature != MISSING && quality.matches("[01459]"))
				context.write(new IntWritable(year), new IntWritable(temperature));
		}
	}

	public static class TemperatureReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int max_temp = 0;
			for (IntWritable val : values) {
				if (max_temp < val.get())
					max_temp = val.get();
			}
			context.write(key, new IntWritable(max_temp));
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "maximum temperature");
		job.setJarByClass(MaximumTemperature.class);
		job.setMapperClass(TemperatureMapper.class);
		job.setCombinerClass(TemperatureReducer.class);
		job.setReducerClass(TemperatureReducer.class);
		// job.setMapOutputKeyClass(IntWritable.class);
		// job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}