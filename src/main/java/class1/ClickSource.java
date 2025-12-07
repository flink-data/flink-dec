package class1;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<Event> {
    private Boolean isRunning = true;
    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();
        String[] users = {"Tom", "Jerry", "Kevin", "Tony", "Bob"};
        String[] urls = {"fav/", "like/", "cart/", "order/", "sign-up/", "sign-in/", "sign-out/"};
        while(isRunning) {
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            long timestamp = Calendar.getInstance().getTimeInMillis();
            ctx.collect(new Event(user, url, timestamp));
            Thread.sleep(1000L);
        }
    }
    @Override
    public void cancel() {
        isRunning = false;
    }
}
