import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task1WikiStatsMapper extends
		Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		System.out.println(" In Combiner now!");

		/**
		 * Create a HasMap containing the wiki project abbreviation and the wiki
		 * project name for a better visualization in the output file
		 */
		final HashMap<String, String> wikiProjectsMap = new HashMap<String, String>() {

			private static final long serialVersionUID = 1L;

			{
				put("", ".wikipedia.org");
				put("b", ".wikibooks.org");
				put("d", ".wiktionary.org");
				put("f", ".wikimediafoundation.org ");
				put("m", ".wikimedia.org");
				put("mw", "wikimobile");
				put("n", ".wikinews.org");
				put("q", ".wikiquote.org");
				put("s", ".wikisource.org");
				put("v", ".wikiversity.org");
				put("voy", ".wikivoyage.org");
				put("w", ".mediawiki.org");
				put("wd", ".wikidata.org");
			};
		};

		/*
		 * "lines" Stores the file's content as lines, in a String array
		 */
		String[] lines = value.toString().split("\\n");

		/*
		 * "noOfViews" is storing the number of views for a specific page(the
		 * value in the 3rd column of the data-set)
		 */
		Long noOfViews;

		/*
		 * "projectCode" stores the code of the wiki project. e.g : ".b", ".f",
		 * etc (see the hash map "wikiProjectsMap" storing the abbreviations and
		 * full project names)
		 */
		String projectCode;
		
		Text newKey = new Text();
		LongWritable newValue = new LongWritable();

		/*
		 * Loop through the file lines
		 */
		for (String line : lines) {
			/*
			 * lineTokens will store the tokens for each line, split by the
			 * unicode value of the character space (e.g: aa.b User:EVula 2
			 * 29324 will be split in lineTokens[] = {"aa.b", "User:EVula", "2",
			 * "29324 "})
			 */
			String[] lineTokens = line.split("\\s+");
						
			if (lineTokens.length > 0) {
				/*
				 * the number of views for a page is stored on the 3rd position
				 * in the file
				 */
				noOfViews = Long.parseLong(lineTokens[2]);
				/*
				 *  get the wiki project code(e.g. "b" for wikibooks, "m" for wikimedia)
				 *  from the first entry in the line array tokens;
				 *  if the token doesn't contain the character ".", 
				 *  it will implicitly refer to the wikipedia project,
				 *   else it will refer to one of the other wiki projects(see wikiProjectsMap above) 
				 */
				if(!lineTokens[0].contains(".")){
					projectCode = "";
				}else{
					projectCode = String.valueOf(lineTokens[0]).substring(
							lineTokens[0].indexOf(".") + 1, lineTokens[0].length());
				}
				/*
				 * check if the project code exist in the hash map previously
				 * defined; replace with the full name of the wiki project; write
				 * the <key, value> in the output file e.g. <.wikibooks.org 2>
				 */
				if (wikiProjectsMap.containsKey(projectCode)) {
					newKey.set(wikiProjectsMap.get(projectCode));					
				}
				
				newValue.set(noOfViews);
				context.write(newKey, new LongWritable(noOfViews));

				
			}
		}

	}
}
