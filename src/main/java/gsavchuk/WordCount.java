package gsavchuk;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {

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
				FSDataInputStream in = localFs.open(fileStatus.getPath());
				try {
					IOUtils.copyBytes(in, out, getConf(), false);
				} finally {
					in.close();
				}
			}
			return 0;
		} finally {
			out.close();
		}
	}
}
