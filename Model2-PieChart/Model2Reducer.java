package secondModel;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;




	public class Model1Reducer extends Reducer<Text	, Consumption, NullWritable, Text>
	{
		@Override
		protected void reduce(Text arg0, Iterable<Consumption> arg1,
				org.apache.hadoop.mapreduce.Reducer<Text, Consumption, NullWritable, Text>.Context arg2)
				throws IOException, InterruptedException {
			try
			{
				double air=0.0d;
				double washer=0.0d;
				double dish=0.0d;
				double dryer=0.0d;
				double furnace=0.0d;
				double kitchen=0.0d;
				double lights=0.0d;
				double living=0.0d;
				double micro=0.0d;
				double total =0.0d;
				
				for(Consumption c : arg1)
				{
					total += Double.parseDouble(c.use.toString());
					air += Double.parseDouble(c.air.toString());
					washer += Double.parseDouble(c.washer.toString());
					dish += Double.parseDouble(c.dish.toString());
					dryer += Double.parseDouble(c.dryer.toString());
					furnace += Double.parseDouble(c.furnace.toString());
					kitchen += Double.parseDouble(c.kitchen.toString());
					lights += Double.parseDouble(c.lights.toString());
					living += Double.parseDouble(c.living.toString());
					micro += Double.parseDouble(c.micro.toString());
				}
				
				air =air*100/total;
				washer = washer*100/total;
				dish = dish*100/total;
				dryer = dryer*100/total;
				furnace = furnace*100/total;
				kitchen = kitchen*100/total;
				lights = lights*100/total;
				living = living*100/total;
				micro = micro*100/total;
				
				arg2.write(NullWritable.get(), new Text(arg0.toString().split("#")[0]+","+arg0.toString().split("#")[1]+","+total+","+air+","+washer+","+dish+","+dryer+","+furnace+","+kitchen+","+lights+","+living+","+micro));
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
		}
	}
	
	
	
	


