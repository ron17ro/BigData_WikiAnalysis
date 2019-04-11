import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WikiStatsDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		/*
		 * JobControl.class - This class encapsulates a set of MapReduce jobs and its dependency.
		 * It tracks the states of the jobs by placing them into different
		 * tables according to their states. This class provides APIs for the
		 * client app to add a job to the group and to get the jobs in the group
		 * in different states. When a job is added, an ID unique to the group
		 * is assigned to the job. This class has a thread that submits jobs
		 * when they become ready, monitors the states of the running jobs, and
		 * updates the states of jobs based on the state changes of their
		 * depending jobs states. The class provides APIs for
		 * suspending/resuming the thread, and for stopping the thread.
		 */
		
		//jobControl will be used to run the MapReduce jobs in sequential order, one at a time, 
		//in order to avoid overlapping, in which case the jobs will fail.
		
		JobControl jobControl = new JobControl("Job Control");
		Configuration conf = new Configuration();
		/*
		 * Validate that two arguments were passed from the command line.
		 */
		if (args.length != 1) {
			System.out.printf("Usage: StubDriver <input dir> \n");
			System.exit(-1);
		}
		System.out.println(" In Driver now!");

		// Instantiate a Job object for the job configuration.

		Job job = Job.getInstance(conf,
				"Task 1 - compute totalviews for each wiki project");
		ControlledJob controlledJob = new ControlledJob(conf);
		controlledJob.setJob(job);
		jobControl.addJob(controlledJob);
		// Set input path given as argument when running the application
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// Set output path hardcoded as "Task1Output" which will be generated
		// when the application will run
		FileOutputFormat.setOutputPath(job, new Path("Task1Output"));

		/*
		 * Specify the jar file that contains your driver, mapper, and reducer.
		 * Hadoop will transfer this jar file to nodes in your cluster running
		 * mapper and reducer tasks.
		 */
		job.setJarByClass(WikiStatsDriver.class);
		job.setMapperClass(Task1WikiStatsMapper.class);
		job.setCombinerClass(Task1WikiStatsCombiner.class);
		job.setReducerClass(Task1WikiStatsReducer.class);

		// Set output <key, value> classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		/*
		 * Specify an easily-decipherable name for the job. This job name will
		 * appear in reports and logs.
		 */
		job.setJobName("Task1 Wiki Stats Driver");

		/*
		 * Start the MapReduce job and wait for it to finish. If it finishes
		 * successfully, return 0. If not, return 1.
		 */
		boolean success = job.waitForCompletion(true);
		Configuration conf1 = new Configuration();
		// Instantiate a Job object for the job configuration.
		Job job1 = Job.getInstance(conf1,
				"Task 2 - compute average views by language");

		ControlledJob controlledJob1 = new ControlledJob(conf1);
		controlledJob1.setJob(job1);
		controlledJob1.addDependingJob(controlledJob);
		jobControl.addJob(controlledJob1);

		// Set input path given as argument when running the application
		FileInputFormat.addInputPath(job1, new Path(args[0]));

		// Set output path
		FileOutputFormat.setOutputPath(job1, new Path("Taks2Output1"));

		/*
		 * Specify the jar file that contains your driver, mapper, and reducer.
		 * Hadoop will transfer this jar file to nodes in your cluster running
		 * mapper and reducer tasks.
		 */
		job1.setJarByClass(WikiStatsDriver.class);
		job1.setMapperClass(Task2WikiStatsMapper.class);
		job1.setCombinerClass(Task2WikiStatsCombiner.class);
		job1.setReducerClass(Task2WikiStatsReducer.class);
		job1.setNumReduceTasks(1);

		// Set output <key, value> classes
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		/*
		 * Specify an easily-decipherable name for the job. This job name will
		 * appear in reports and logs.
		 */
		job1.setJobName("Task2 Wiki Stats Driver");

		/*
		 * Start the MapReduce job and wait for it to finish. If it finishes
		 * successfully, return 0. If not, return 1.
		 */
		success = job1.waitForCompletion(true);		
		
		Configuration conf2 = new Configuration();
		// Instantiate a Job object for the job configuration.
		Job job2 = Job.getInstance(conf2,
				"Task 2 - sort results in descending order by average views");
		ControlledJob controlledJob2 = new ControlledJob(conf2);
		controlledJob2.addDependingJob(controlledJob1);
		controlledJob2.setJob(job2);		
		jobControl.addJob(controlledJob2);
		
		// Set input path
		FileInputFormat.addInputPath(job2, new Path("Taks2Output1"));

		// Set output path
		FileOutputFormat.setOutputPath(job2, new Path("Task2Output2"));

		/*
		 * Specify the jar file that contains your driver, mapper, and reducer.
		 * Hadoop will transfer this jar file to nodes in your cluster running
		 * mapper and reducer tasks.
		 */
		job2.setJarByClass(WikiStatsDriver.class);
		job2.setMapperClass(Task2SortWikiStatsMapper.class);
		job2.setReducerClass(Task2SortWikiStatsReducer.class);
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setSortComparatorClass(DoubleComparator.class);

		job2.setMapOutputKeyClass(DoubleWritable.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setNumReduceTasks(1);

		/*
		 * Specify an easily-decipherable name for the job. This job name will
		 * appear in reports and logs.
		 */
		job2.setJobName("Task2 Sort Wiki Stats Driver");

		

		Thread jobControlThread = new Thread(jobControl);
		jobControlThread.start();
		/*
		 * Start the MapReduce job and wait for it to finish. If it finishes
		 * successfully, return 0. If not, return 1.
		 */
		success = job2.waitForCompletion(true);
		return (success ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new WikiStatsDriver(), args);
		System.exit(exitCode);

	}

}
