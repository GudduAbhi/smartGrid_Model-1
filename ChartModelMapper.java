package chartModel;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;




public class ChartModelMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Splitter tokenSplitter;
	
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		tokenSplitter = Splitter.on(",").trimResults();
	}
	
	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		try
		{
			List<String> list = Lists.newArrayList(tokenSplitter.split(value.toString()));
			
			if("localminute".equalsIgnoreCase(list.get(0)))return; //ignore the header
			
			Consumption consumption = new Consumption(new Text(list.get(2)));
			//Text usage = new Text(list.get(2)); //Get the usage value, i.e. 3rd index of the array
			
			Date date = new SimpleDateFormat("yyyy-MM-dd").parse(list.get(0).split(" ")[0]);
			GregorianCalendar gc = new GregorianCalendar();
			gc.setTime(date);
			//int week = gc.get(Calendar.WEEK_OF_MONTH);
			int month = gc.get(Calendar.MONTH);
			
			context.write(new Text("Month-"+month+"#"+list.get(1)),consumption);
			
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	

}
