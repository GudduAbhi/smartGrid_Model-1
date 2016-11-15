package chartModel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Consumption implements Writable{

	public Text use;
		
	
	public Consumption()
	{
		use=new Text();
	}
	
	public Consumption(Consumption c)
	{
		use=c.use;
	}
	
	public Consumption(Text use)
	{
		this.use=use;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.use.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.use.write(out);
	}

	
}



