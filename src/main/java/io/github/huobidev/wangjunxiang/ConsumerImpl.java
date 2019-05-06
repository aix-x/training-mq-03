package io.github.huobidev.wangjunxiang;

import com.alibaba.fastjson.JSONObject;
import io.github.huobidev.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class ConsumerImpl implements Consumer {
    private KafkaConsumer<String, String> consumer;
    private static final String TOPIC = "aixTest";
    private Set<String> oriderId = new HashSet<>();
    private List<Order> orderList = new ArrayList<>();

    public ConsumerImpl() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "aix");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        consumer.assign(Arrays.asList(new TopicPartition(TOPIC, 0)));

    }

    @Override
    public Long consume(Long offset) {
        consumer.seek(new TopicPartition(TOPIC, 0), offset);
        ConsumerRecords<String, String> records = consumer.poll(1000L);
        for (ConsumerRecord<String, String> record : records) {
            //去重
            if (oriderId.contains(record.key())) {
                continue;
            }
            oriderId.add(record.key());
            Order order = JSONObject.parseObject(record.value(), Order.class);
            orderList.add(order);
            offset = record.offset();
        }
        //返回offset,以便下次获取
        return offset;
    }

    public static void main(String[] args) {
        new ConsumerImpl().consume(0L);
    }
}
