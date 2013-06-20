package gsavchuk;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class NumberText implements WritableComparable<NumberText> {
	private int number;
	private Text text;

	public NumberText() {
	}

	public NumberText(int number, Text text) {
		this.number = number;
		this.text = text;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(number);
		text.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		number = in.readInt();
		text = new Text();
		text.readFields(in);
	}

	@Override
	public int compareTo(NumberText o) {
		int res = Integer.compare(number, o.number);
		if (res != 0)
			return res;
		return text.compareTo(o.text);
	}

	public int getNumber() {
		return number;
	}

	public Text getText() {
		return text;
	}

	@Override
	public String toString() {
		return Integer.toString(number) + "," + text;
	}

}
