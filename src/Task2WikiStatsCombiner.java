import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The class Task2WikiStatsCombiner will perform an initial aggregation of the page views
 * and it will rewrite the <key, value> pair as following:
 * after the value aggregation, the output pair will be written as 
 * <language, averagePageViews_counter> where "averagePageViews" is the intermediate computing 
 *  of the average and "counter" is the number of unique occurrences(lines) for a language 
 */
public class Task2WikiStatsCombiner extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		System.out.println(" In Reducer now!");
		
		Double result = 0D;
		Double pageViewsCount = 0D;		
		int counter = 0;
		
		
		for (Text value : values) {
			/*
			 * see if the file was already aggregated in a pair <key, a_b> by checking the presence
			 * of the "_" in the value. Recompute the total page views
			 * by multiplying a*b and adding the value to the pageViewCount
			 *  
			 * If the value wasn't already aggregated then parse the value
			 *  and aggregate it to the total pageViewsCount 
			 */
			if(value.toString().contains("_")) {
				String[] averageTokens = value.toString().split("_");
				counter += Integer.parseInt(averageTokens[1].toString());
				pageViewsCount += Double.parseDouble(averageTokens[0].toString()) * counter;
				
			}else {					
				pageViewsCount += Double.parseDouble(value.toString());
				counter++;
			}
					
		}
		/*
		 * compute the average and leave only 2 decimal places by formating the string 
		 */
		result = Double.parseDouble(String.format( "%.2f", pageViewsCount / counter));				
		
		context.write(key, new Text(result + "_" + counter));

	}

}
