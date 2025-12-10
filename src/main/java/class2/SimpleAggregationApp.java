package class2;

import class1.ClickSource;
import class1.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SimpleAggregationApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Tom", "fav/", 1000L),
                new Event("Tom", "cart/", 2000L),
                new Event("Nancy", "fav/", 3000L),
                new Event("Jerry", "fav/", 4000L),
                new Event("Tom", "home/", 5000L),
                new Event("Carl", "like/", 700L),
                new Event("Tom", "fav/", 600L)
        );
        stream.keyBy(value -> "global")
                //.max("timestamp").print();
                        //.maxBy("timestamp").print();
                                .minBy("timestamp").print();
        env.execute();
    }
}
