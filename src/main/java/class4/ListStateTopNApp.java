package class4;

import class1.ClickSource;
import class1.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
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

public class ListStateTopNApp {

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
                }));
        SingleOutputStreamOperator<UrlWindowCount> urlCountStream =
                stream.keyBy(data -> data.url)
                        .window(SlidingEventTimeWindows.of(Time.seconds(60),  Time.seconds(10)))
                        .aggregate(new UrlViewCountAggregator(), new UrlWindowCountResultProcessor());
        urlCountStream
                .keyBy(data -> data.windowEnd)
                        .process(new TopNProcessor(3))
                                .print();
        env.execute();
    }

    public static class TopNProcessor extends KeyedProcessFunction<Long, UrlWindowCount, String> {
        private final int topSize;
        private transient ListState<UrlWindowCount> urlWindowCountListState;

        public TopNProcessor(int topSize) {
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<UrlWindowCount> descriptor = new ListStateDescriptor<UrlWindowCount>("urlWindowCountListState", UrlWindowCount.class);
            urlWindowCountListState = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void processElement(UrlWindowCount value, KeyedProcessFunction<Long, UrlWindowCount, String>.Context ctx, Collector<String> out) throws Exception {
            urlWindowCountListState.add(value);
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<UrlWindowCount> all = new ArrayList<>();
            for (UrlWindowCount urlWindowCount : urlWindowCountListState.get()) {
                all.add(urlWindowCount);
            }
            urlWindowCountListState.clear();
            all.sort(new Comparator<UrlWindowCount>() {
                @Override
                public int compare(UrlWindowCount o1, UrlWindowCount o2) {
                    return Math.toIntExact(o2.count - o1.count);
                }
            });
            StringBuilder sb = new StringBuilder();
            sb.append("======================================================\n");
            sb.append("Window End: ").append(new Timestamp(ctx.getCurrentKey())).append('\n');
            for (int i = 0; i < Math.min(all.size(), topSize); i++) {
                UrlWindowCount current = all.get(i);
                String info = "\n " + "No. " + (i + 1) + " Url: "
                        + current.url + " View Count: " + current.count + " \n";
                sb.append(info);
            }
            sb.append("======================================================\n");
            out.collect(sb.toString());
        }
    }

    public static class UrlViewCountAggregator implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }
        @Override
        public Long add(Event value, Long accumulator) {
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

    public static class UrlWindowCountResultProcessor
            extends ProcessWindowFunction<Long, UrlWindowCount, String, TimeWindow> {
        @Override
        public void process(String url, ProcessWindowFunction<Long, UrlWindowCount, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<UrlWindowCount> out) throws Exception {
            long count = elements.iterator().next();
            out.collect(new UrlWindowCount(url,count, context.window().getStart(), context.window().getEnd()));
        }
    }


    public static class UrlWindowCount {
        @Override
        public String toString() {
            return "UrlWindowCount{" +
                    "url='" + url + '\'' +
                    ", count=" + count +
                    ", windowStart=" + windowStart +
                    ", windowEnd=" + windowEnd +
                    '}';
        }

        public UrlWindowCount() {
        }

        public UrlWindowCount(String url, long count, long windowEnd, long windowStart) {
            this.url = url;
            this.count = count;
            this.windowEnd = windowEnd;
            this.windowStart = windowStart;
        }

        public String url;
        public long count;
        public long windowStart;
        public long windowEnd;
    }
}

    /* Event
    // key by url
    * url - window - count urlWindowCount
    * key by windowEnd
    *
    * End:
    ontimer register windowEnd fire
    * */
