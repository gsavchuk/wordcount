package gsavchuk;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class NormalizerMapper extends MapReduceBase implements
		Mapper<Text, IntWritable, Text, IntWritable> {
	private Text word = new Text();
	
	@Override
	public void map(Text key, IntWritable value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		String afterStrip = retainLettersIn(key.toString());
		if (!afterStrip.isEmpty()) {
			word.set(afterStrip);
			output.collect(word, value);
		}
	}

	private String retainLettersIn(String string) {
		StringBuilder sb = new StringBuilder(string.length());
		for (char ch : string.toCharArray()) {
			if (Character.isLetter(ch))
				sb.append(ch);
		}
		return sb.toString();
	}
}
