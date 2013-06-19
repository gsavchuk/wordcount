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
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {
	public static class TokenizerMapper extends MapReduceBase implements
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String afterStrip = retainLettersIn(itr.nextToken());
				if (!afterStrip.isEmpty()) {
					word.set(afterStrip);
					output.collect(word, one);
				}
			}
		}

		String retainLettersIn(String string) {
			StringBuilder sb = new StringBuilder(string.length());
			for (char ch : string.toCharArray()) {
				if (Character.isLetter(ch))
					sb.append(ch);
			}
			return sb.toString();
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
		JobConf conf = new JobConf(getConf(), WordCount.class);
		conf.setNumReduceTasks(3);
		conf.setPartitionerClass(ConstantPartitioner.class);
		conf.setJobName("wordcount");

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobConf countStage = new JobConf(false);
		ChainMapper.addMapper(conf, TokenizerMapper.class, Object.class,
				Text.class, Text.class, IntWritable.class, false, countStage);

		JobConf reduceStage = new JobConf(false);
		ChainReducer.setReducer(conf, IntSumReducer.class, Text.class,
				IntWritable.class, Text.class, IntWritable.class, false,
				reduceStage);

		JobClient.runJob(conf);
		return 0;
	}
}
