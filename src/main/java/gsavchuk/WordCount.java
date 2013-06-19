package gsavchuk;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SkipBadRecords;

public class WordCount {
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

	public static class ThrowsExceptionMapper extends MapReduceBase implements
			Mapper<Text, IntWritable, Text, IntWritable> {

		public void map(Text key, IntWritable value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String word = key.toString();
			if (word.startsWith("a")) {
				throw new RuntimeException("crashed at word: " + word);
			}
			output.collect(key, value);
		}
	}

	public static class IntSumReducer extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String word = key.toString();
			if (word.startsWith("a")) {
				throw new RuntimeException("crashed at word: " + word);
			}
			int sum = 0;
			while (values.hasNext()) {
				IntWritable intWritable = values.next();
				sum += intWritable.get();

			}
			result.set(sum);
			output.collect(key, result);
		}
	}

	/**
	 * 
	 * 
	 * @param args
	 * 
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("wordcount");
	    
		conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(IntWritable.class);
	    conf.setMapperClass(TokenizerMapper.class);
	    conf.setReducerClass(IntSumReducer.class);

	    FileInputFormat.addInputPath(conf, new Path(args[0]));
	    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	    
	    SkipBadRecords.setAttemptsToStartSkipping(conf, 1);
		SkipBadRecords.setMapperMaxSkipRecords(conf, Long.MAX_VALUE);
		SkipBadRecords.setReducerMaxSkipGroups(conf, Long.MAX_VALUE);

//		JobConf countStage = new JobConf(false);
//		ChainMapper.addMapper(conf, TokenizerMapper.class, Object.class,
//				Text.class, Text.class, IntWritable.class, false, countStage);
//
//		JobConf reduceStage = new JobConf(false);
//		ChainReducer.setReducer(conf, IntSumReducer.class, Text.class,
//				IntWritable.class, Text.class, IntWritable.class, false,
//				reduceStage);

//		ChainReducer.addMapper(conf, ThrowsExceptionMapper.class, Text.class,
//				IntWritable.class, Text.class, IntWritable.class, false, null);

		JobClient.runJob(conf);
	}
}
