import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Task2SortWikiStatsReducer will write the pairs resulted after the sorting phase in an output file
 * as <averagePageViews, language>
 */

public class Task2SortWikiStatsReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {


  @Override
  public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

    for (Text value : values) {
        context.write(key, value);
    }
  }
}
