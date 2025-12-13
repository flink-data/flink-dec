package class3;

import class1.ClickSource;
import class1.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

public class WindowProcessApp {

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
        //ProcessFunction
        //Unique URL View Count, Sliding Window -> 1 minute window slide 10s

        stream.keyBy(data -> true)
                .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(10)))
                .process(new UniqueViewCount())
                .print();
        env.execute();
    }

    public static class UniqueViewCount extends ProcessWindowFunction<Event, String, Boolean, TimeWindow> {
        @Override
        public void process(Boolean aBoolean,
                            ProcessWindowFunction<Event, String, Boolean, TimeWindow>.Context context,
                            Iterable<Event> elements, Collector<String> out) throws Exception {
            HashSet<String> set = new HashSet<>();
            for (Event event : elements) {
                set.add(event.url);
            }
            Integer uniqueView = set.size();
            Long startTime = context.window().getStart();
            Long endTime = context.window().getEnd();
            out.collect("===================================================\n");
            out.collect("Window Start: " + new Timestamp(startTime) +
                    " Window End: " + new Timestamp(endTime) + " Unique View Count: " + uniqueView);
            out.collect("===================================================\n");
        }
    }
}
