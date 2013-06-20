package gsavchuk;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SortReducer extends MapReduceBase implements
		Reducer<NumberText, Text, Text, Text> {

	@Override
	public void reduce(NumberText key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		if (!values.hasNext())
			throw new IllegalStateException("no value for key: " + key);
		Text val = values.next();
		if (values.hasNext())
			throw new IllegalStateException("more than 1 value for key: " + key);
		output.collect(key.getText(), val);
	}

}
