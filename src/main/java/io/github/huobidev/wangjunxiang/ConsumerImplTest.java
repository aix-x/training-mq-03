package io.github.huobidev.wangjunxiang;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ConsumerImplTest {

    Consumer consumer = new ConsumerImpl();
    @Test
    public void consume() {
        consumer.consume(0L);
    }
}