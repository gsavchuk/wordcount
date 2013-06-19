package gsavchuk;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class StripPunctuationMapper extends MapReduceBase implements
		Mapper<Object, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	@Override
	public void map(Object key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			String afterStrip = retainLettersAndNumbersIn(itr.nextToken());
			if (!afterStrip.isEmpty()) {
				word.set(afterStrip);
				output.collect(word, one);
			}
		}

	}

	private String retainLettersAndNumbersIn(String string) {
		StringBuilder sb = new StringBuilder(string.length());
		for (char ch : string.toCharArray()) {
			if (Character.isLetterOrDigit(ch))
				sb.append(ch);
		}
		return sb.toString();
	}
}
