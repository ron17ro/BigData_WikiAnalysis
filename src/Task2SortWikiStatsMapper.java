import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * Task2SortWikiStatsMapper will take as an input the files produced 
 * by the Task2WikiStatsReducer class and it will reverse the order
 *  of the key and value <language, averagePageViews>  which will result in 
 * <averagePageViews,language>. In the next phase, the MapREduce framework will use 
 * the custom DoubleComparator class to sort in descending order the averagePageViews
 *
 */
public class Task2SortWikiStatsMapper extends Mapper<Text, Text, DoubleWritable, Text> {
	
	  DoubleWritable averagePageViews = new DoubleWritable();
	  
	  @Override
	  public void map(Text key, Text value, Context context)
	      throws IOException, InterruptedException {
		  
		  System.out.println(" In Task2 Sort Mapper now!");
		  
		  Double newVal = Double.parseDouble(value.toString());
		    averagePageViews.set(newVal);
		    context.write(averagePageViews, key);

	  }
}
	  
	
