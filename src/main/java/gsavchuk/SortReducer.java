package gsavchuk;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

public class SortReducer extends MapReduceBase implements
		Reducer<NumberText, Text, Text, Text> {

	private MultipleOutputs multipleOutputs;
	private IntWritable numOfCited = new IntWritable();

	@Override
	public void configure(JobConf job) {
		multipleOutputs = new MultipleOutputs(job);
	}

	@Override
	public void close() throws IOException {
		multipleOutputs.close();
	}

	@SuppressWarnings("unchecked")
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
		numOfCited.set(key.getNumber());
		multipleOutputs.getCollector(WordCount.NUMBER_OF_CITED, reporter)
				.collect(key.getText(), numOfCited);
	}

}
