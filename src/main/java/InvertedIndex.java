import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private FileSplit split;
        private Pattern pattern = Pattern.compile("\\b(\\w)+\\b", Pattern.CASE_INSENSITIVE);

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            split = (FileSplit) context.getInputSplit();
            Matcher matcher = pattern.matcher(value.toString());
            String[] filepath = split.getPath().toString().split("/");
            String path = filepath[filepath.length-1];
            Long start = key.get();
            while (matcher.find()) {
                Long pos = start + matcher.start();
                context.write(new Text(matcher.group().toLowerCase()), new Text(path + ":" + pos));
            }
        }
    }

    public static class IndexReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> list = new ArrayList<String>();
            for (Text text: values) {
                Collections.addAll(list, text.toString().split(","));
            }
            Collections.sort(list, new Comparator<String>() {
                public int compare(String o1, String o2) {
                    String[] s1 = o1.split(":"), s2 = o2.split(":");
                    if (!s1[0].equals(s2[0]))
                        return s1[0].compareTo(s2[0]);
                    return Integer.parseInt(s1[1]) - Integer.parseInt(s2[1]);
                }
            });
            context.write(key, new Text(Integer.toString(list.size()) + " " + list.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndex.TokenizerMapper.class);
        job.setReducerClass(InvertedIndex.IndexReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // job.setCombinerClass(IndexCombiner.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
