package class3;

import class1.ClickSource;
import class1.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.time.Duration;

public class WindowAggregateApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        // {Tom, 3} {Lucy, 4}
        stream.keyBy(event -> event.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new clickAggregator())
                .print();
        env.execute();
    }

    public static class clickAggregator implements AggregateFunction<Event, Tuple3<String, Integer, Long>,String> {
        @Override
        public Tuple3<String, Integer, Long> createAccumulator() {
            return Tuple3.of("", 0, 0L);
        }
        @Override
        public Tuple3<String, Integer, Long> add(Event value, Tuple3<String, Integer, Long> accumulator) {
            return Tuple3.of(value.user, accumulator.f1 + 1, value.timestamp);
        }
        @Override
        public String getResult(Tuple3<String, Integer, Long> accumulator) {
            Timestamp lastClickTime = new Timestamp(accumulator.f2);
            return "User: " + accumulator.f0 + ", Last Click Time: " + lastClickTime + ", Click Count: " + accumulator.f1;
        }
        @Override
        public Tuple3<String, Integer, Long> merge(Tuple3<String, Integer, Long> a, Tuple3<String, Integer, Long> b) {
            return null;
        }
    }
}
