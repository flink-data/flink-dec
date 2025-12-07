package class1;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream dataStream = environment.readTextFile("C:\\Users\\admin\\IdeaProjects\\flink-dec\\src\\main\\resources\\wordcount.txt");

        // <cat,cat, dog ,fish ,fish ,fish ,fish ,dog ,dog>
        // <cat, 1> <cat, 2>  <dog, 1>    .......   <dog, 3>
        //string ------------------------- Tuple2<String, Integer>------
        // <cat, 1> ,<cat, 1>  ||  <dog, 1> <dog, 1> <dog, 1> \\ ....<>
         //       transformation                       aggregation
        dataStream.flatMap(new MyFlatMapper()).keyBy(0).sum(1).print();
        environment.execute();
    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        //input: "cat cat dog fish fish fish fish dog dog"
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word: words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
