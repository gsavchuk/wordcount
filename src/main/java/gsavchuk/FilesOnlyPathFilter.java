package gsavchuk;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class FilesOnlyPathFilter implements PathFilter {

	private final FileSystem fs;

	public FilesOnlyPathFilter(FileSystem fs) {
		this.fs = fs;
	}

	@Override
	public boolean accept(Path path) {
		try {
			return fs.isFile(path);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

}
