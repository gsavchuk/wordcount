package gsavchuk;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class FilePerLetterOutputFormat extends
		MultipleTextOutputFormat<Text, IntWritable> {
	@Override
	protected String generateFileNameForKeyValue(Text key, IntWritable value,
			String name) {
		char firstChar = (char) key.charAt(0);
		char lowerChar = Character.toLowerCase(firstChar);
		return new StringBuilder(5).append(lowerChar).append(".txt").toString();
	}

}
