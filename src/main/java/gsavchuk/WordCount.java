package gsavchuk;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {
	private static final String TEMP_STORAGE = "/tmp/1";

	public static class TokenizerMapper extends MapReduceBase implements
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				output.collect(word, one);
			}
		}
	}

	public static class IntSumReducer extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				IntWritable intWritable = values.next();
				sum += intWritable.get();
			}
			result.set(sum);
			output.collect(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new WordCount(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		JobConf tokenizeConf = new JobConf(getConf(), WordCount.class);
		tokenizeConf.setJobName("tokenize job");
		tokenizeConf.setOutputKeyClass(Text.class);
		tokenizeConf.setOutputValueClass(IntWritable.class);
		tokenizeConf.setMapperClass(TokenizerMapper.class);
		tokenizeConf.setReducerClass(IntSumReducer.class);
		tokenizeConf.setOutputFormat(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(tokenizeConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(tokenizeConf, new Path(TEMP_STORAGE));

		JobConf normalizeConf = new JobConf(getConf(), WordCount.class);
		normalizeConf.setJobName("normalize job");
		normalizeConf.setOutputKeyClass(Text.class);
		normalizeConf.setOutputValueClass(IntWritable.class);
		normalizeConf.setMapperClass(NormalizerMapper.class);
		normalizeConf.setReducerClass(IntSumReducer.class);
		normalizeConf.setInputFormat(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(normalizeConf, new Path(TEMP_STORAGE));
		FileOutputFormat.setOutputPath(normalizeConf, new Path(args[1]));

		Job tokenizeJob = new Job(tokenizeConf);
		Job normalizeJob = new Job(normalizeConf);
		normalizeJob.addDependingJob(tokenizeJob);

		JobControl jobControl = new JobControl("word count");
		jobControl.addJob(tokenizeJob);
		jobControl.addJob(normalizeJob);
		jobControl.run();
		return 0;
	}
}
