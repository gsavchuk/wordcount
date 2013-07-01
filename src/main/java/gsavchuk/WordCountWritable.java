package gsavchuk;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

public class WordCountWritable implements Writable, DBWritable {

	private String word;
	private int count;

	@Override
	public void write(PreparedStatement statement) throws SQLException {
		statement.setString(1, word);
		statement.setInt(2, count);
	}

	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, word);
		out.writeInt(count);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		word = Text.readString(in);
		count = in.readInt();
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

}
