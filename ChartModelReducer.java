package chartModel;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;






public class ChartModelReducer extends Reducer<Text	, Consumption, NullWritable, Text>{


	@Override
	protected void reduce(Text arg0, Iterable<Consumption> arg1,
			org.apache.hadoop.mapreduce.Reducer<Text, Consumption, NullWritable, Text>.Context arg2)
			{
			try
		{
			double total =0.0d;
			//for the same month, aggregate values
			for(Consumption c : arg1)
			{
				total += Double.parseDouble(c.use.toString());

			}
				
			arg2.write(NullWritable.get(),new Text(arg0.toString().split("#")[0]+","+arg0.toString().split("#")[1]+","+total));
		}

			catch(Exception ex)
		{
			ex.printStackTrace();
		}

		}
}
