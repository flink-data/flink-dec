package class1;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Arrays;

public class ReadSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<String> dataStream1 = environment.fromElements("red", "white", "green");
        DataStream<Integer> dataStream2 = environment.fromElements(1, 2, 3);
        DataStream<Integer> dataStream3 = environment.fromCollection(Arrays.asList(4,5,6));
        DataStreamSource<Long> dataStreamSource = environment.generateSequence(7,10);
        //dataStream1.print();
        //dataStream2.print();
        //dataStream3.print();
        //dataStreamSource.print();

        DataStreamSource<Event> dataStreamSource2 = environment.addSource(new ClickSource());
        dataStreamSource2.print();
        environment.execute();
    }
}
