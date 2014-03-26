import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Writable;


public class PartialString implements Writable {
	
	private String parString;
	private Integer loInteger;
	public PartialString(String parString,Integer loInteger){
		this.parString = parString;
		this.loInteger = loInteger;
	}
	
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		String[] group = in.toString().split(",");
		parString = group[0];
		loInteger = Integer.parseInt(group[1]);
		
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.toString());
		
	}
	public void setString(String str){
		parString = str;
	}
	public void setlo(Integer lo){
		loInteger = lo;
	}

	@Override
	public String toString() {
		return parString+","+loInteger;
	}
	
	

}
