public class PartialString{

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

}
