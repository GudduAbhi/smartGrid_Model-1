package chartModel;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;





public class ChartModelDriver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		FileSystem fs = null;
		String uri = arg0[0];
		
		try
		{
			Job job = new Job(getConf());
			job.setJobName("ChartModel");
			job.setJarByClass(ChartModelDriver.class);
			
			Configuration conf  = job.getConfiguration();
			/*conf.addResource(new Path("core-site.xml"));
			conf.addResource(new Path("hdfs-site.xml"));*/
			
			fs = FileSystem.get(URI.create(uri), conf);
			if(fs.exists(new Path(arg0[0]+"/electricityconsumption/LineChartModel5")))
				fs.delete(new Path(arg0[0]+"/electricityconsumption/LineChartModel5"));
			
			job.setMapperClass(ChartModelMapper.class);
			job.setReducerClass(ChartModelReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Consumption.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(arg0[0]+"/electricityconsumption/input"));
			FileOutputFormat.setOutputPath(job, new Path(arg0[0]+"/electricityconsumption/LineChartModel5"));
			
			return job.waitForCompletion(true)?1:0;
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			return 0;
		}
		finally
		{
			if(fs!=null)
				fs.close();
		}
	}

	public static void main(String[] args) {
		try
		{
			ToolRunner.run(new Configuration(),new ChartModelDriver(), args);
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
}

