package dsps2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ExtractedPair implements WritableComparable<ExtractedPair> {

	public Text w1;
	public Text w2;

	public ExtractedPair(){
		w1 = new Text();
		w2 = new Text();
	}
	
	public ExtractedPair(ExtractedPair p){
		w1 = new Text(p.w1);
		w2 = new Text(p.w2);
	}
	
	public ExtractedPair(Text word1, Text word2){
		set(word1,word2);
	}
	
	public void set(Text word1, Text word2){
		w1 = new Text(word1);
		w2 = new Text(word2);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		w1.readFields(in);
		w2.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		w1.write(out);
		w2.write(out);
	}

	@Override
	public int compareTo(ExtractedPair other) {
		int first = w1.compareTo(other.w1);
		if (first == 0){
			return w2.compareTo(other.w2);
		}
		return first;
	}

	public String toString(){
		return w1.toString()+","+w2.toString();
	}
	
}
