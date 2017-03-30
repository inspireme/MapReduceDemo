package jp.whisper.hadoop.mrdemo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class JoinValueWritable implements Writable {

	private Text content;
	private byte tag;

	public JoinValueWritable(Text content, byte tag) {
		super();
		this.content = content;
		this.tag = tag;
	}
	public JoinValueWritable(){
		this.content = new Text();
		this.tag = (byte)0;
	}

	public void readFields(DataInput input) throws IOException {
		tag = input.readByte();
		content.readFields(input);
	}

	public void write(DataOutput output) throws IOException {
		output.writeByte(tag);
		content.write(output);
	}

	public Text getContent() {
		return content;
	}

	public void setContent(Text content) {
		this.content = content;
	}

	public byte getTag() {
		return tag;
	}

	public void setTag(byte tag) {
		this.tag = tag;
	}

}
