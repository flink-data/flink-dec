package class4;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

public class PageViewTopNApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<UserBehavior> stream = env.readTextFile("/Users/wya/IdeaProjects/flink-dec/src/main/resources/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new UserBehavior(arr[0], arr[1], arr[2], arr[3], Long.parseLong(arr[4]) * 1000L);
                    }
                }).filter(data -> data.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        stream.keyBy(r -> r.itemId)
                .window(SlidingEventTimeWindows.of(Time.minutes(60), Time.minutes(5)))
                .aggregate(new CountAggregator(), new ItemViewCountProcessor())
                        .keyBy(r -> r.windowEnd)
                                .process(new TopNItemProcessor(5)).print();
        env.execute();
    }

    public static class TopNItemProcessor extends KeyedProcessFunction<Long, ItemViewCount, String> {

        private Integer size;
        private ListState<ItemViewCount> itemViewCountListState;

        public TopNItemProcessor(Integer size) {
            this.size = size;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<>("ItemViewCountListState", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount value, KeyedProcessFunction<Long, ItemViewCount, String>.Context ctx, Collector<String> out) throws Exception {
            itemViewCountListState.add(value);
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 100L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            ArrayList<ItemViewCount> itemViewCounts = new ArrayList<>();
            for (ItemViewCount itemViewCount : itemViewCountListState.get()) {
                itemViewCounts.add(itemViewCount);
            }
            itemViewCountListState.clear();
            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.count.intValue() - o1.count.intValue();
                }
            });
            StringBuilder sb = new StringBuilder();
            sb.append("======================================================\n");
            sb.append("Window End: ").append(new Timestamp(timestamp)).append('\n');
            for (int i = 0; i < Math.min(size, itemViewCounts.size()); i++) {
                ItemViewCount current = itemViewCounts.get(i);
                String info = "\n " + "No. " + (i + 1) + " ItemId: "
                        + current.itemId + " View Count: " + current.count + " \n";
                sb.append(info);
            }
            sb.append("======================================================\n");
            out.collect(sb.toString());
        }
    }


    public static class ItemViewCountProcessor extends ProcessWindowFunction<Long, ItemViewCount, String, TimeWindow> {
        @Override
        public void process(String itemId, ProcessWindowFunction<Long, ItemViewCount, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<ItemViewCount> out) throws Exception {
            out.collect(new ItemViewCount(itemId, context.window().getEnd(), elements.iterator().next()));
        }
    }

    public static class CountAggregator implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }
        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
        }
        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }
        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }
}
