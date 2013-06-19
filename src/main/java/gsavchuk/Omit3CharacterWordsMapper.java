package gsavchuk;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Omit3CharacterWordsMapper extends MapReduceBase implements
		Mapper<Text, IntWritable, Text, IntWritable> {
	private Text word = new Text();

	@Override
	public void map(Text key, IntWritable value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		String strKey = key.toString();
		if (strKey.length() >= 3) {
			word.set(strKey);
			output.collect(word, value);
		}
	}
}
