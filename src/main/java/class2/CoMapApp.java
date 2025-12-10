package class2;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class CoMapApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> data1 = env.fromElements("mobile_1",  "mobile_2", "mobile_3", "mobile_4", "mobile_5");
        DataStreamSource<String> data2 = env.fromElements("web_1", "web_2", "web_3", "web_4", "web_5");
        DataStreamSource<Integer> data3 = env.fromElements(1,2,3,4,5);
        DataStreamSource<Long> data4 = env.fromElements(6L,7L,8L,9L,10L);
        DataStream<String> result = data3.connect(data4).map(new CoMapFunction<Integer, Long, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "Source 1: " + value.toString();
            }

            @Override
            public String map2(Long value) throws Exception {
                return "Source 2: " + value.toString();
            }
        });
        data1.union(data2).print();
        result.print();
        env.execute();
    }
}
