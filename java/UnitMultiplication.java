package main.java;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Doing "Transition Matrix" multiplies "PR Matrix" to get "subPR" for further sum-up calculation.
 */
public class UnitMultiplication {
  /**
   * Transition matrix.
   * Done in mapper.
   */
  public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString().trim();
      String[] fromTo = line.split("\t");  // cuz MapReduce defaults as tab

      if (fromTo.length < 2) {  // if the "from" starting point is a dead-end -> no other points connected
        return;
      }

      String from = fromTo[0];
      String tos[] = fromTo[1].split(",");
      for (String to : tos) {
        // 1/tos.length is the probabilities of each node to another node (ex: node 1 has 3 connections -> prob: 1/3)
        context.write(new Text(from), new Text(to + "=" + (double)1 / tos.length));
      }
    }
  }

  /**
   * PR matrix.
   * Done in mapper.
   */
  public static class PRMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] pr = value.toString().trim().split("\t");
      context.write(new Text(pr[0]), new Text(pr[1]));
    }
  }

  /**
   * Do multiplication.
   * Done in reducer.
   */
  public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      List<String> transitionUnit = new ArrayList<String>();
      double prUnit = 0;

      for (Text value : values) {
        if (value.toString().contains("=")) {
          transitionUnit.add(value.toString());
        } else {
          prUnit = Double.parseDouble(value.toString());  // add PR weight at the end of the list, so that we can utilize it later
        }
      }

      for (String unit : transitionUnit) {
        String outputKey = unit.split("=")[0];
        double relation = Double.parseDouble(unit.split("=")[1]);
        String outputValue = String.valueOf(relation * prUnit);  // multiplication process
        context.write(new Text(outputKey), new Text((outputValue)));
      }
    }
  }

  public static void main(String[] args) throws Exception {

    Configuration config = new Configuration();
    Job job = Job.getInstance(config);
    job.setJarByClass(UnitMultiplication.class);

    job.setReducerClass(MultiplicationReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    job.waitForCompletion(true);
  }
}
