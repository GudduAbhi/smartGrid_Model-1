package chartModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.LineReader;

public class NLineInputFormat extends FileInputFormat<LongWritable, Text>{

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		arg1.setStatus(arg0.toString());
		return new LineRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext arg0) throws IOException {
		List<InputSplit> splits = new ArrayList<InputSplit>();
		for(FileStatus status : listStatus(arg0))
		{
			splits.addAll(getSplitForFile(status,arg0.getConfiguration()));
		}
		return splits;
	}
	
	private List<InputSplit> getSplitForFile(FileStatus status , Configuration conf ) throws IOException
	{
		List<InputSplit> splits = new ArrayList<InputSplit>();
		Path fileName = status.getPath();
		
		if(status.isDir())
			throw new IOException("Input is directory and not file");
	
		LineReader lr = null;
		FileSystem fs = fileName.getFileSystem(conf);
		
		try
		{
			FSDataInputStream in = fs.open(fileName);
			lr = new LineReader(in, conf);
			Text line = new Text();
			
			int num = -1;
			int numberLines = 0;
			long length = 0;
			long begin = 0;
			
			while((num = lr.readLine(line))>0)
			{
				numberLines++;
				length+=num;
				
				if(numberLines==10)
				{
					if(begin==0)
						splits.add(new FileSplit(fileName, begin, length-1, new String[]{}));
					else
						splits.add(new FileSplit(fileName, begin-1, length, new String[]{}));
					
					begin+=length;
					length=0;
					numberLines=0;
				}
			}
			if(numberLines!=0)
				splits.add(new FileSplit(fileName, begin, length, new String[]{}));
			return splits;
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			return null;
		}
		finally
		{
			if(lr!=null)lr.close();
			if(fs!=null)fs.close();
		}
	}
}

