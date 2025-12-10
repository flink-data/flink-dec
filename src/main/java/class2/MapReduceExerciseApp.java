package class2;

import class1.ClickSource;
import class1.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashSet;
import java.util.Set;

public class MapReduceExerciseApp {

    public static void main(String[] args) throws Exception {
        //1. reduce and print user's unique URL
        //2. print user visit max number of url
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //<Event> -> <Tom, {fav/ cart/ like/}
        //<Tom, {fav/ }, <Tom, {cart/}>, <Tom, {fav/}>
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        SingleOutputStreamOperator<UserUrlSet> userUrls = stream
                .map(new MapFunction<Event, UserUrlSet>() {
                    @Override
                    public UserUrlSet map(Event value) throws Exception {
                        Set<String> set = new HashSet<>();
                        set.add(value.url);
                        return new UserUrlSet(value.user, set);
                    }
                });
        SingleOutputStreamOperator<UserUrlSet> urlPerUser = userUrls
                .keyBy(value -> value.user)
                .reduce(new ReduceFunction<UserUrlSet>() {
                    @Override
                    public UserUrlSet reduce(UserUrlSet value1, UserUrlSet value2) throws Exception {
                        value1.urls.addAll(value2.urls);
                        return value1;
                    }
                });
        urlPerUser.print();
        env.execute();
    }
}
