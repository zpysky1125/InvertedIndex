import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex2 {

    // Combiner is useless, since it will not decrease the size of mapper output

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
            //TODO: combine path to key and create partitioner to make it auto sort
            while (matcher.find()) {
                Long pos = start + matcher.start();
                context.write(new Text(matcher.group().toLowerCase() + ":" + path + ":" + pos), new Text(path + ":" + pos));
            }
        }
    }

    private static class TextPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text text, Text text2, int i) {
            return (text.toString().split(":")[0].hashCode() & Integer.MAX_VALUE) % i;
        }
    }

    private static class GroupComparator extends WritableComparator {
        protected GroupComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            String s1 = a.toString().split(":")[0];
            String s2 = b.toString().split(":")[0];
            return s1.compareTo(s2);
        }
    }

    private static class SortComparator extends WritableComparator {
        protected SortComparator() {
            super(Text.class, true);
        }
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            String[] s1 = a.toString().split(":");
            String[] s2 = b.toString().split(":");
            if (!s1[0].equals(s2[0]))
                return s1[0].compareTo(s2[0]);
            if (!s1[1].equals(s2[1]))
                return s1[1].compareTo(s2[1]);
            return Integer.parseInt(s1[2]) - Integer.parseInt(s2[2]);
        }
    }

    public static class IndexReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> list = new ArrayList<String>();
            for (Text text: values)
                list.add(text.toString());
            context.write(new Text(key.toString().split(":")[0]), new Text(Integer.toString(list.size()) + " " + list.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index 2");
        job.setJarByClass(InvertedIndex2.class);
        job.setMapperClass(InvertedIndex2.TokenizerMapper.class);
        job.setReducerClass(InvertedIndex2.IndexReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(TextPartitioner.class);
        job.setSortComparatorClass(SortComparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
