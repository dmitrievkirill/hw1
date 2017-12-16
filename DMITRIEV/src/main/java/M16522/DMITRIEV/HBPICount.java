package M16522.DMITRIEV;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.*;


public class HBPICount extends Configured implements Tool {
	/**
	 * @param command-line
	 *            arguments input folder url and output folder url in hdfs.
	 */
	public static void main(String args[]) throws Exception {
		// running the job with toolrunner
		int res = ToolRunner.run(new Configuration(), new HBPICount(), args);
		System.exit(res);
	}

	/**
	 * Runs the job with its Configuration.
	 */
	public int run(String[] args) throws Exception {

		// creating the job
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "HBPICount");
		job.setJarByClass(HBPICount.class);

		// setting classes for Map-Reduce
		job.setMapperClass(Map.class);
		
		job.setReducerClass(Reduce.class);

		// adding files to distributed cache
		job.addCacheFile(new URI("/hw1/city.en.txt"));
		job.addCacheFile(new URI("/hw1/region.en.txt"));

		// Specify input/output key/value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		// setting output format as sequence file
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// specifying input output paths
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Running the job
		job.waitForCompletion(true);
		if (job.isSuccessful()) {
			return 0;
		} else
			return 1;
	}

	/**
	 * Class for mapping input key/value to intermediate key/value.
	 */
	public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {
		private final static IntWritable one = new IntWritable(1);

		// columns in input files for definite attributes
		private static final int MIN_BID_PRICE = 250;
		private static final int BID_INDEX = 19;
		private static final int CITY_INDEX = 7;
		private static final int MAX_INDEX = 24;
		private static final int OP_INDEX = 4;

		/**
		 * Maps input key/value to intermediate key/value - high bid priced impressions.
		 * 
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] columns = value.toString().split("\\t");
			if (columns.length < MAX_INDEX) {
				return;
			}
			try {
				int cityId = Integer.parseInt(columns[CITY_INDEX]);
				int bidPrice = Integer.parseInt(columns[BID_INDEX]);
				String operatingSystem = columns[OP_INDEX];
				IntWritable keyOut = new IntWritable(cityId);
				if (bidPrice > MIN_BID_PRICE) {
					context.write(keyOut, one);
				}
			} catch (NumberFormatException e) {
			}
		}
	}


	/**
	 * Class for mapping intermediate key/value to final key/value.
	 */
	public static class Reduce extends Reducer<IntWritable, IntWritable, Text, IntWritable> {

		HashMap<Integer, String> cityList = new HashMap<Integer, String>();

		/**
		 * Reads mapping files from disributed cache and puts it into hashmap before
		 * starting reduce.
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

			// using distributed cache
			URI[] cacheFiles = context.getCacheFiles();
			if (cacheFiles != null && cacheFiles.length > 0) {
				BufferedReader reader = null;
				BufferedReader reader1 = null;
				try {
					FileSystem fs = FileSystem.get(context.getConfiguration());
					Path path = new Path(cacheFiles[0].toString());
					reader = new BufferedReader(new InputStreamReader(fs.open(path)));
					String line;
					while ((line = reader.readLine()) != null) {
						String[] columns = line.toString().split("\\s+");
						cityList.put(Integer.parseInt(columns[0]), columns[1]);
					}
					Path path1 = new Path(cacheFiles[1].toString());
					reader1 = new BufferedReader(new InputStreamReader(fs.open(path1)));
					while ((line = reader1.readLine()) != null) {
						String[] columns = line.toString().split("\\s+");
						cityList.put(Integer.parseInt(columns[0]), columns[1]);
					}
				} catch (IOException e) {
					// Ignore malformed lines.
				} catch (NumberFormatException e) {
					// Ignore malformed lines.
				} finally {
					try {
						reader.close();
						reader1.close();
					} catch (IOException e) {
						// Ignore malformed lines.
					}
				}
			}
		}

		/**
		 * Counts amount of equals key - calculates amount of high-bid priced
		 * impressions per city.
		 */
		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			Text outKey;
			for (IntWritable count : counts) {
				sum += count.get();
			}
			if (!cityList.isEmpty()) {
				String cityName = cityList.get(key);
				if (cityName == null) {
					outKey = new Text("UNKNOWN");
				} else {
					outKey = new Text(cityName);
				}
				context.write(outKey, new IntWritable(sum));
			}
			System.out.print("Cached file is not found\n\r");
		}
	}

}
