package gsavchuk;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class OmitWordsLessThan5TimesMapper extends MapReduceBase implements
		Mapper<Text, IntWritable, Text, IntWritable> {

	public void map(Text key, IntWritable value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		if (value.get() >= 5) {
			output.collect(key, value);
		}
	}
}