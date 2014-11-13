package org.techmytalk.sortingbyvalue;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;


public class SortingByValue {

	public static class SampleMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, Text> {
		private Text text = new Text();
		private IntWritable intW = new IntWritable();

		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] splitValue;
			if (line != null && !line.equalsIgnoreCase("")) {
				splitValue = line.split(",");
				text.set(line);
				intW.set(Integer.parseInt(splitValue[1]));
				output.collect(intW, text);
			}

		}
	}

	private static class NaturalKeyGroupingComparator extends
			WritableComparator {

		/**
		 * Constructor.
		 */
		protected NaturalKeyGroupingComparator() {
			super(IntWritable.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			return -1 * a.compareTo(b);

		}

	}

	public static void main(String[] args) throws Exception {

		FileUtils.deleteDirectory(new File("/Local/data/output"));

		JobConf conf = new JobConf(SortingByValue.class);
		conf.setJobName("SampleMapReduce");
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(SampleMapper.class);
		conf.setOutputKeyComparatorClass(NaturalKeyGroupingComparator.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TopFileOutPutFormat.class);

		FileInputFormat.setInputPaths(conf,
				new Path("/Local/data/employee.csv"));
		FileOutputFormat.setOutputPath(conf, new Path("/Local/data/output"));
		JobClient.runJob(conf);
	}
	
	private static class TopFileOutPutFormat extends MultipleTextOutputFormat<IntWritable,Text>{
		@Override
		protected IntWritable generateActualKey(IntWritable key, Text value) {
			// TODO Auto-generated method stub
			return null;
		}
		
		
	}

}