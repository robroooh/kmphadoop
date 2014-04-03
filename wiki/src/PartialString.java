import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PartialString implements Writable{

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
	public PartialString() {
		this.patString = null;
		this.loInteger = null;
		this.BigFile = null;
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

	public void write(DataOutput out) throws IOException {
		System.out.println("Out Bitch");
		out.writeChars(this.BigFile+","+this.patString+","+this.loInteger.toString());
	}

	public void readFields(DataInput in) throws IOException {
		String[] st = in.readLine().split(",");
		System.out.println("IN Bitch");
		this.BigFile = st[0];
		this.patString = st[1];
		this.loInteger = Integer.parseInt(st[2]);
		
	}

}
