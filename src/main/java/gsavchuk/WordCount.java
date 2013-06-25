package gsavchuk;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {
	private static final String COMMA = ",";
	private final static Pattern pattern = Pattern
			.compile("^(?<custId>\\d+),(?<rating>\\d),.*");

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new WordCount(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}

		FileSystem localFs = FileSystem.getLocal(getConf());
		FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:9000/"),
				getConf());

		Path toHdfsPath = new Path(args[1]);
		toHdfsPath = toHdfsPath.makeQualified(hdfs);

		Path fromLocalPath = new Path(args[0]);
		fromLocalPath = fromLocalPath.makeQualified(localFs);

		if (hdfs.exists(toHdfsPath)) {
			System.err.println("file exists: " + toHdfsPath + " , exiting...");
			return 1;
		}
		FSDataOutputStream out = hdfs.create(toHdfsPath);
		try {
			FileStatus[] listStatus = localFs.listStatus(fromLocalPath,
					new FilesOnlyPathFilter(localFs));
			for (FileStatus fileStatus : listStatus) {
				parseFile(localFs, out, fileStatus);
			}
			return 0;
		} finally {
			out.close();
		}
	}

	private void parseFile(FileSystem fs, FSDataOutputStream out,
			FileStatus fileStatus) throws IOException {
		BufferedReader in = new BufferedReader(new InputStreamReader(
				fs.open(fileStatus.getPath())));
		try {
			String movieId = null;
			String line;
			while ((line = in.readLine()) != null) {
				if (null == movieId) {
					movieId = line.substring(0, line.indexOf(":"));
					continue;
				}
				Matcher matcher = pattern.matcher(line);
				if (matcher.matches()) {
					String toWrite = movieId + COMMA + matcher.group("custId")
							+ COMMA + matcher.group("rating")
							+ System.lineSeparator();
					out.write(toWrite.getBytes());
				}
			}
		} finally {
			in.close();
		}
	}
}
