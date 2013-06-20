package gsavchuk;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
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

	public static class InvertedIndexMapper extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			output.collect(value, key);
		}
	}

	public static class InvertedIndexReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		Text result = new Text();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringBuilder sb = new StringBuilder();
			while (values.hasNext()) {
				Text t = values.next();
				if (sb.length() != 0)
					sb.append(",");
				sb.append(t.toString());
			}
			result.set(sb.toString());
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
		conf.setJobName("inverted index");
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobConf buildIndexConf = new JobConf(getConf(), WordCount.class);
		buildIndexConf.setJobName("inverted index");
		buildIndexConf.setInputFormat(KeyValueTextInputFormat.class);
		buildIndexConf.set("key.value.separator.in.input.line", ",");
		buildIndexConf.setOutputKeyClass(Text.class);
		buildIndexConf.setOutputValueClass(Text.class);
		buildIndexConf.setMapperClass(InvertedIndexMapper.class);
		buildIndexConf.setReducerClass(InvertedIndexReducer.class);
		buildIndexConf.setOutputFormat(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(buildIndexConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(buildIndexConf, new Path(TEMP_STORAGE));

		JobConf sortConf = new JobConf(getConf(), WordCount.class);
		sortConf.setJobName("sort results");
		sortConf.setOutputKeyClass(Text.class);
		sortConf.setOutputValueClass(Text.class);
		sortConf.setMapOutputKeyClass(NumberText.class);
		sortConf.setMapperClass(SortMapper.class);
		sortConf.setReducerClass(SortReducer.class);
		sortConf.setInputFormat(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(sortConf, new Path(TEMP_STORAGE));
		FileOutputFormat.setOutputPath(sortConf, new Path(args[1]));

		Job tokenizeJob = new Job(buildIndexConf);
		Job normalizeJob = new Job(sortConf);
		normalizeJob.addDependingJob(tokenizeJob);

		JobControl jobControl = new JobControl("word count");
		jobControl.addJob(tokenizeJob);
		jobControl.addJob(normalizeJob);
		jobControl.run();
		return 0;
	}
}
