package main.java;

import java.io.IOException;
import java.text.DecimalFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Get the multiplication result to do sum-up and refresh PR weight.
 * Use new PR weight to do the next calculation until converged.
 */
public class UnitSum {
  /**
   * Get the multiplication result.
   * From straight perspective, we could do the sum on reducer without mapper,
   * but actually a mapper is necessary, while reducer is optional,
   * thus we still need a mapper to get the result from the former MapReducer and pass the result to current MapReducer's reducer.
   */
  public static class PassMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] pageSubRank = value.toString().split("\t");
      context.write(new Text(pageSubRank[0]), new DoubleWritable(Double.parseDouble(pageSubRank[1])));
    }
  }

  /**
   * Do the sum-up and record.
   */
  public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
      double sum = 0;
      for (DoubleWritable value : values) {
        sum += value.get();
      }
      DecimalFormat df = new DecimalFormat("#.0000");
      sum = Double.valueOf(df.format(sum));
      context.write(key, new DoubleWritable(sum));
    }
  }

  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);
    job.setJarByClass(UnitSum.class);

    job.setMapperClass(PassMapper.class);
    job.setReducerClass(SumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
  }
}
