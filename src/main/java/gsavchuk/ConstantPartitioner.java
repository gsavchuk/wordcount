package gsavchuk;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class ConstantPartitioner implements Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		if (3 != numPartitions)
			throw new IllegalStateException("expected 3 partitions");
		char firstChar = (char) key.charAt(0);
		char firstLower = Character.toLowerCase(firstChar);
		if (Character.compare(firstLower, 'h') <= 0)
			return 0;
		if (Character.compare('s', firstLower) <= 0)
			return 2;
		return 1;
	}

	@Override
	public void configure(JobConf job) {
	}
}
