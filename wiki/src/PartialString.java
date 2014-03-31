import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Writable;

public class PartialString implements Writable {

	private String patString;
	private Integer loInteger;
	private String BigFile;

	public String getBigFile() {
		return BigFile;
	}

	public void setBigFile(String bigFile) {
		BigFile = bigFile;
	}

	public PartialString(String patString, Integer loInteger) {
		this.patString = patString;
		this.loInteger = loInteger;
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		String[] group = in.toString().split(",");
		patString = group[0];
		loInteger = Integer.parseInt(group[1]);

	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.toString());
	}

	public String getPatString() {
		return patString;
	}

	public void setPatString(String patString) {
		this.patString = patString;
	}

	public Integer getLoInteger() {
		return loInteger;
	}

	public void setLoInteger(Integer loInteger) {
		this.loInteger = loInteger;
	}

	@Override
	public String toString() {
		return patString + "," + loInteger;
	}

}
