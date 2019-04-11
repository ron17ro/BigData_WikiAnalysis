import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CopyOfTask2WikiStatsReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		
		DoubleWritable result = new DoubleWritable();
		Double averagePageViews = 0D;
		Double viewsCount = 0D;		
		int counter = 0;
		System.out.println(" In Reducer now!");
		for (DoubleWritable value : values) {
			viewsCount += value.get();
			counter++;
		}
		averagePageViews = viewsCount/counter;
		result.set(averagePageViews);
		context.write(key, result);
		System.out.println("number of total views : " + counter);
		
	}

}
