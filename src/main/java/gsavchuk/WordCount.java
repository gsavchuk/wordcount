package gsavchuk;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {

	private static final String CACHED_FILE = "/home/gsavchuk/downloads/apat63_99.txt";

	public static class InvertedIndexMapper extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {
		private final static Pattern pattern = Pattern
				.compile("^(?<patent>\\d+),(?<year>\\d+),\\d*,\\d*,(?<country>.+?),.*");
		private final Map<String, String> patentToCountry = new HashMap<String, String>();
		private final Text k = new Text();
		private final Text v = new Text();

		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String cited = value.toString();
			String citedCountry = patentToCountry.get(cited);
			if (citedCountry == null) {
				return;
			}
			k.set(citedCountry);
			v.set(cited);
			output.collect(k, v);
		}

		@Override
		public void configure(JobConf job) {
			try {
				URI[] files = DistributedCache.getCacheFiles(job);
				Path path = new Path(files[0]);
				if (!path.toString().endsWith(CACHED_FILE))
					throw new IllegalStateException(
							"expected cached file to exist: " + CACHED_FILE
									+ " , found: " + path);
				readFileIntoMap(job, path);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		private void readFileIntoMap(JobConf job, Path path) throws IOException {
			FileSystem fs = path.getFileSystem(job);
			BufferedReader in = new BufferedReader(new InputStreamReader(
					fs.open(path)));
			try {
				String line;
				while ((line = in.readLine()) != null) {
					Matcher matcher = pattern.matcher(line);
					if (!matcher.matches())
						continue;
					int year = Integer.parseInt(matcher.group("year"));
					if (year >= 1980) {
						patentToCountry.put(matcher.group("patent"),
								matcher.group("country"));
					}
				}
			} finally {
				in.close();
			}
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
		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.set("key.value.separator.in.input.line", ",");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(InvertedIndexMapper.class);
		conf.setReducerClass(InvertedIndexReducer.class);
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		DistributedCache.addCacheFile(new Path(CACHED_FILE).toUri(), conf);
		JobClient.runJob(conf);
		return 0;
	}
}
