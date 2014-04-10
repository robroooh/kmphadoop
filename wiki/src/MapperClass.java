import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.omg.CORBA.PRIVATE_MEMBER;

public class MapperClass extends Mapper<Text, Text, Text, Text> {

	private ArrayList<String> patt;
	private ArrayList<Integer> loInteger;
	private Scanner scan;

	private static final Integer SPLIT_LENGTH = 16777216 + 99;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		
		patt = new ArrayList<String>();

		URI[] cache = context.getCacheFiles();

		FileInputStream fis = new FileInputStream(cache[0].getPath());
		scan = new Scanner(fis);
		
		while (scan.hasNext()) {
			patt.add(scan.nextLine());
		}

	}

	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		for (int index = 0; index < patt.size(); index++) {

			searchSubString(value.toString().toCharArray(), patt.get(index)
					.toCharArray());
						
			if (loInteger.size() > 0) {
				for (int i = 0; i < loInteger.size(); i++) {
					context.write(
							new Text(key.toString() + "," + patt.get(index)),
							new Text(loInteger.get(i).toString()));
					
				}
			} else {
				context.write(new Text(key.toString() + "," + patt.get(index)),
						new Text());
			}

		}
	}

	public int[] preProcessPattern(char[] ptrn) {
		int i = 0, j = -1;
		int ptrnLen = ptrn.length;
		int[] b = new int[ptrnLen + 1];

		b[i] = j;
		while (i < ptrnLen) {
			while (j >= 0 && ptrn[i] != ptrn[j]) {
				// if there is mismatch consider next widest border
				j = b[j];
			}
			i++;
			j++;
			b[i] = j;
		}
		return b;
	}

	public void searchSubString(char[] text, char[] ptrn) {
		loInteger = new ArrayList<Integer>();
		text = Arrays.copyOf(text, SPLIT_LENGTH-99 + ptrn.length-1);
		int i = 0, j = 0;
		// pattern and text lengths
		int ptrnLen = ptrn.length;
		int txtLen = text.length;
		// initialize new array and preprocess the pattern
		int[] b = preProcessPattern(ptrn);

		while (i < txtLen) {
			while (j >= 0 && text[i] != ptrn[j]) {
				j = b[j];
			}
			i++;
			j++;

			// a match is found
			if (j == ptrnLen) {
				loInteger.add(i - ptrnLen);
				j = b[j];
			}
		}
	}
}