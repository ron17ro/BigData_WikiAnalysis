import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task2WikiStatsMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		System.out.println(" In Mapper now!");

		/*
		 * "lines" Stores the file's content as lines, in a String array
		 */
		String[] lines = value.toString().split("\\n");

		/*
		 * "noOfViews" is storing the number of views for a specific page(the
		 * value in the 3rd column of the data-set)
		 */
		Double noOfViews = 0d;

		/*
		 * "projectCode" stores the code of the wiki project. e.g : ".b", ".f",
		 * etc (see the hash map "wikiProjectsMap" storing the abbreviations and
		 * full project names)
		 */
		String languageCode;

		Text newKey = new Text();
		Text newValue = new Text();

		/*
		 * loop through the file's lines
		 */
		for (String line : lines) {
			/*
			 * split the line by spaces in tokens 
			 */
			String[] lineTokens = line.split("\\s+");

			if (lineTokens.length > 0) {
				/*
				 * parse the number of views as Double
				 */
				noOfViews = Double.parseDouble(lineTokens[2]);
				/*
				 * get the language code from the first token of the line
				 * 
				 */
				if (!lineTokens[0].contains(".")) {
					languageCode = String.valueOf(lineTokens[0]);
				} else {
					languageCode = String.valueOf(lineTokens[0]).substring(0,
							lineTokens[0].indexOf("."));
				}
				
				/*
				 * set the values for the output <key, value> and write them in the context object;
				 * the objects newKey and newValue are declared only once to avoid creating multiple objects
				 * in the mapper phase 
				 */
				newKey.set(languageCode);
				newValue.set(String.valueOf(noOfViews));
				context.write(newKey, newValue);

			}
		}

	}
}
