package class1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMapJob {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStreamSource<Event> dataStreamSource2 = environment.addSource(new ClickSource());
        dataStreamSource2.flatMap(new FlatMapFunction<Event, Tuple2<String, String>>() {
            @Override
            public void flatMap(Event value, Collector<Tuple2<String, String>> out) throws Exception {
                //if event.url equal fav/ . out put url + name
                //if event.url equal like/  out put url + name + like
            }
        });

        //keyby
    }
}
