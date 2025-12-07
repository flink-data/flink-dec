package class1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatColor {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<String> stream = environment.fromElements("red", "white", "green");
        //if color == red, out red * 2; if color == white, out white * 3; if color == green, out none
        stream.flatMap(new FlatColorMapper()).print();
        environment.execute();
    }
    public static class FlatColorMapper implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            if (value.equals("red")) {
                out.collect("red");
                out.collect("red");
            } else if (value.equals("white")) {
                out.collect("white");
                out.collect("white");
                out.collect("white");
            }
        }
    }
}
