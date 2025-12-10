package class2;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapFilterApp {
    public static class User {
        String name;
        Integer age;
        public User() {
        }
        public User(String name, Integer age) {
            this.name = name;
            this.age = age;
        }
        @Override
        public String toString() {
            return "User{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }

    public static class DetailUser {
        String name;
        Integer age;
        String tag;
        public DetailUser() {
        }
        public DetailUser(String name, Integer age, String tag) {
            this.name = name;
            this.age = age;
            this.tag = tag;
        }
        @Override
        public String toString() {
            return "DetailUser{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    ", tag='" + tag + '\'' +
                    '}';
        }
    }

    //User: <String name, Integer age>
    //DetailUser: <String name, Integer age, tag: (Adult, teenager, child)
    //User -> Map -> DetailUser -> Filter if DetailUser.tag == adult, return false;
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<User> dataStreamSource = env.fromElements(
                new User("Ben", 30),
                new User ("Tom", 17),
                new User("Carl", 8)
        );
        SingleOutputStreamOperator<DetailUser> dataStream = dataStreamSource
                .map(new MapFunction<User, DetailUser>() {
                    @Override
                    public DetailUser map(User value) throws Exception {
                        if (value.age >= 18) {
                            return new DetailUser(value.name, value.age, "adult");
                        }
                        else if (value.age < 18 && value.age >= 10) {
                            return new DetailUser(value.name, value.age, "teenager");
                        }
                        else {
                            return new DetailUser(value.name, value.age, "kid");
                        }
                    }
                });
        //dataStream.print();
        DataStream<DetailUser> result = dataStream.filter(new FilterFunction<DetailUser>() {
            @Override
            public boolean filter(DetailUser value) throws Exception {
               return value.tag != "adult";
            }
        });
        result.print();
        env.execute();
    }
}
