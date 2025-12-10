package class2;

import class1.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichMapFunctionApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Tom", "fav/", 1000L),
                new Event("Tom", "cart/", 2000L),
                new Event("Nancy", "fav/", 3000L),
                new Event("Jerry", "fav/", 4000L),
                new Event("Tom", "home/", 5000L),
                new Event("Carl", "like/", 700L),
                new Event("Tom", "fav/", 600L)
        );
        stream.map(new RichMapFunction<Event, Integer>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("Start of life cycle: " + getRuntimeContext().getIndexOfThisSubtask());
            }

            @Override
            public Integer map(Event value) throws Exception {
                return value.url.length();
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("End of life cycle: " + getRuntimeContext().getIndexOfThisSubtask());
            }
        }).print();
        env.execute();
    }
}
