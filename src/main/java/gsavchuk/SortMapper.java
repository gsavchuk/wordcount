package gsavchuk;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SortMapper extends MapReduceBase implements
		Mapper<Text, Text, NumberText, Text> {

	@Override
	public void map(Text key, Text value,
			OutputCollector<NumberText, Text> output, Reporter reporter)
			throws IOException {
		String[] strings = value.toString().split(",");
		output.collect(new NumberText(strings.length, key), value);
	}
}
