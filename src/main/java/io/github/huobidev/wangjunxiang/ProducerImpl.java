package io.github.huobidev.wangjunxiang;

import com.alibaba.fastjson.JSON;
import io.github.huobidev.Order;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerImpl implements Producer {

    private KafkaProducer<String, String> kafkaProducer;

    private static final String TOPIC = "aixTest";

    public ProducerImpl() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //设置分区类,根据key进行数据分区
        kafkaProducer = new KafkaProducer<>(props);
    }

    @Override
    public void produce(Order order) {
        ProducerRecord record = new ProducerRecord(TOPIC, order.getId().toString(), JSON.toJSONString(order));
        kafkaProducer.send(record);
        kafkaProducer.flush();
        kafkaProducer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        for (int i = 0 ;i<5000;i++){
            Thread.sleep(1000);
            Order order = new Order();
            order.setId(Long.valueOf(i));
            order.setPrice(11.11);
            order.setSymbol("btcusdt");
            order.setTs(1L);
            new ProducerImpl().produce(order);
        }
    }
}
