package carmodel;

import java.text.DecimalFormat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ModelCarReducer extends Reducer<Text, Consumption, NullWritable, Text>{

	@Override
	protected void reduce(Text arg0, Iterable<Consumption> arg1,
			org.apache.hadoop.mapreduce.Reducer<Text, Consumption, NullWritable, Text>.Context arg2)
			{
		
		try
		{
			/*double air_avg =0.0d;
			double washer_avg =0.0d;
			double dish_avg =0.0d;
			double dryer_avg =0.0d;
			double furnace_avg =0.0d;
			double kitchen_avg =0.0d;
			double lights_avg =0.0d;
			double living_avg =0.0d;
			double micro_avg =0.0d;
			*/
			double total_car=0.0d;
			double total_app=0.0d;
			DecimalFormat df = new DecimalFormat("#0.00");
			String car,total;
			
			//int air_i=0,air_c=0;
			int car_i=0,car_c=0;
		/*	int washer_i=0,washer_c=0;
			int dish_i=0,dish_c=0;
			int dryer_i=0,dryer_c=0;
			int furnace_i=0,furnace_c=0;
			int kitchen_i=0,kitchen_c=0;
			int lights_i=0,lights_c=0;
			int living_i=0,living_c=0;
			int micro_i=0,micro_c=0;
		*/	//for the same month, aggregate values
			
			for(Consumption c : arg1)
			{
				if (Double.parseDouble(c.car.toString())!=0d) {car_i++;} //count no. of hours for car charging
				total_car += Double.parseDouble(c.car.toString());
				
				//total += Double.parseDouble(c.use.toString());
				total_app  += Double.parseDouble(c.air.toString()) + Double.parseDouble(c.washer.toString()) + Double.parseDouble(c.dish.toString()) + Double.parseDouble(c.dryer.toString()) + Double.parseDouble(c.furnace.toString()) + Double.parseDouble(c.kitchen.toString()) + Double.parseDouble(c.lights.toString())+ Double.parseDouble(c.living.toString())+Double.parseDouble(c.micro.toString());
			}
			
			car = df.format(total_car);
			total = df.format(total_app);
			
			/*air_avg = air_avg/air_c;
			washer_avg = washer_avg/washer_c;
			dish_avg = dish_avg/dish_c;
			dryer_avg = dryer_avg/dryer_c;
			furnace_avg = furnace_avg/furnace_c;
			kitchen_avg = kitchen_avg/kitchen_c;
			lights_avg = lights_avg/lights_c;
			living_avg = living_avg/living_c;
			micro_avg = micro_avg/micro_c;
			*/
			
			//arg2.write(NullWritable.get(), new Text (arg0 +","+air_i+","+air_avg+","+washer_i+","+washer_avg+","+dish_i+","+dish_avg+","+dryer_i+","+dryer_avg+","+furnace_i+","+furnace_avg+","+kitchen_i+","+kitchen_avg+","+lights_i+","+lights_avg+","+living_i+","+living_avg+","+micro_i+","+micro_avg));
			arg2.write(NullWritable.get(), new Text (arg0 +","+total+","+car+","+car_i));
			//arg2.write(NullWritable.get(),new Text(arg0.toString().split("#")[0]+","+arg0.toString().split("#")[1]+","+total));
		}
		
			catch(Exception ex)
		{
			ex.printStackTrace();
		}
			}
	


}
