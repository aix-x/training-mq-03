package io.github.huobidev.wangjunxiang;

import io.github.huobidev.Order;
import org.junit.Test;

import static org.junit.Assert.*;

public class ProducerImplTest {

    Producer producer = new ProducerImpl();
    @Test
    public void produce() {
        Order order = new Order();
        order.setTs(1L);
        order.setId(1L);
        order.setSymbol("btcusdt");
        order.setPrice(11.11);
        producer.produce(order);

    }
}