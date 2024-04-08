//Kept much stuff from UnigramFrequency to make this code easier to write
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.*;

public class BigramsFrequency {
  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
    // private final static IntWritable one = new IntWritable(1);
    private Text doc_id = new Text();
    private Text word_extracted = new Text();

    List<String> list = new ArrayList<>();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      //Getting ID and content
      String[] splits = value.toString().split("\\t", 2);
      doc_id.set(splits[0]);
      String doc_content = splits[1];
      doc_content = doc_content.toLowerCase();
      doc_content = doc_content.replaceAll("[^a-zA-Z]+", " ");
      StringTokenizer itr = new StringTokenizer(doc_content);
      String prev = itr.nextToken();

      while (itr.hasMoreTokens()) {
        String single_word = itr.nextToken();
        if (single_word.trim().isEmpty()) {
          continue;
        }

        if (list.size() == 0) {
          list.add("bruce willis");
          list.add("computer science");
          list.add("information retrieval");
          list.add("los angeles");
          list.add("power politics");
        }

        word_extracted.set(prev + " " + single_word);
        if (list.contains(prev + " " + single_word)) {
        context.write(word_extracted, doc_id);
        }
        // context.write(word, documentId);

        prev = single_word;
      }
    }
  }


  //taken hint from gpt and stackoverflow
  public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
    // private IntWritable result = new IntWritable();

    public void reduce(Text word, Iterable<Text> documentIDs, Context context)
        throws IOException, InterruptedException {
      Map<String, Integer> docFrequency = new HashMap();
      for (Text documentID : documentIDs) {
        String docID = documentID.toString();
        docFrequency.put(docID, docFrequency.getOrDefault(docID, 0) + 1);
      }

      StringBuilder docIdWordFrequencies = new StringBuilder();
      for (Map.Entry<String, Integer> entry : docFrequency.entrySet()) {
        if (docIdWordFrequencies.length() > 0) {
          docIdWordFrequencies.append(" ");
        }
        String docId = entry.getKey();
        Integer wordFrequency = entry.getValue();
        String docIdWordFrequency = String.format("%s:%d", docId, wordFrequency);
        docIdWordFrequencies.append(docIdWordFrequency);
      }

      context.write(word, new Text(docIdWordFrequencies.toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");

    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

//Old WordCount code which I overwrote to make UnigramFrequency code work
// public class WordCount {
//    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
//    {
//       private final static IntWritable one = new IntWritable(1);
//       private Text word = new Text();

//       public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
//       {
//          StringTokenizer itr = new StringTokenizer(value.toString());
//          while (itr.hasMoreTokens()) 
//          {
//             word.set(itr.nextToken());
//             context.write(word, one);
//          }
//       }
//    }

//    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
//    {
//       private IntWritable result = new IntWritable();
//       public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
//       {
//          int sum = 0;
//          for (IntWritable val : values) 
//          {
//             sum += val.get();
//          }
//          result.set(sum);
//          context.write(key, result);
//       }
//    }
