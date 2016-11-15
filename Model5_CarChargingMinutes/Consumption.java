package carmodel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Consumption implements Writable{

	public Text time;
	public Text dataId;
	public Text use;
	public Text air;
	public Text car;
	public Text washer;
	public Text dish;
	public Text dryer;
	public Text furnace;
	public Text kitchen;
	public Text lights;
	public Text living;
	public Text micro;
	
	
	public Consumption()
	{
		time=new Text();
		dataId=new Text();
		use=new Text();
		air=new Text();
		car=new Text();
		washer=new Text();
		dish=new Text();
		dryer=new Text();
		furnace=new Text();
		kitchen=new Text();
		lights=new Text();
		living=new Text();
		micro=new Text();
	}
	
	public Consumption(Consumption c)
	{
		time=c.time;
		dataId=c.dataId;
		use=c.use;
		air=c.air;
		car=c.car;
		washer=c.washer;
		dish=c.dish;
		dryer=c.dryer;
		furnace=c.furnace;
		kitchen=c.kitchen;
		lights=c.lights;
		living=c.living;
		micro=c.micro;
	}
	
	public Consumption(Text time,Text dataId ,Text use,Text air,Text car,Text washer ,Text dish,Text dryer,Text furnace , Text kitchen,Text lights, Text living
			,Text micro)
	{
		this.time=time;
		this.dataId=dataId;
		this.use=use;
		this.air=air;
		this.car=car;
		this.washer=washer;
		this.dish=dish;
		this.dryer=dryer;
		this.furnace=furnace;
		this.kitchen=kitchen;
		this.lights=lights;
		this.living=living;
		this.micro=micro;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.time.readFields(in);
		this.dataId.readFields(in);
		this.use.readFields(in);
		this.air.readFields(in);
		this.car.readFields(in);
		this.washer.readFields(in);
		this.dish.readFields(in);
		this.dryer.readFields(in);
		this.furnace.readFields(in);
		this.kitchen.readFields(in);
		this.lights.readFields(in);
		this.living.readFields(in);
		this.micro.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.time.write(out);
		this.dataId.write(out);
		this.use.write(out);
		this.air.write(out);
		this.car.write(out);
		this.washer.write(out);
		this.dish.write(out);
		this.dryer.write(out);
		this.furnace.write(out);
		this.kitchen.write(out);
		this.lights.write(out);
		this.living.write(out);
		this.micro.write(out);
	}

	
}
