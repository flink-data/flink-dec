package class2;

import class1.ClickSource;
import class1.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapReduceApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        SingleOutputStreamOperator<Tuple2<String, Long>> clicks = stream
                .map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event value) throws Exception {
                        return Tuple2.of(value.user, 1L);
                    }
                });
        //clicks.print();
        //# clicks by each user
        SingleOutputStreamOperator<Tuple2<String, Long>> clicksByEachUser = clicks
                .keyBy(tuple -> tuple.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                });
        //clicksByEachUser.print("Total Clicks by each User:  ");
        //Top clicks User
        SingleOutputStreamOperator<Tuple2<String, Long>> topClicksUser = clicksByEachUser
                .keyBy(value -> "global")
                        .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                            //<tom,3> <Nancy,2>
                            @Override
                            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                                if (value1.f1 > value2.f1) {
                                    return value1;
                                } else {
                                    return value2;
                                }
                            }
                        });
        topClicksUser.print("Top clicks user: ");
        env.execute();
    }
}
