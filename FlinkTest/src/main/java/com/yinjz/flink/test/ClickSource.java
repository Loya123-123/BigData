package com.yinjz.flink.test;

import com.yinjz.flink.operator_5.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<Event> {
    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        Random random = new Random();
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};
        while (true){

            sourceContext.collect(new Event(users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()));
            // 隔1秒生成一个点击事件，方便观测
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {

    }
}
