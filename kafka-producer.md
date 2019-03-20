### kafka如何发送消息

#### kafka发送消息的流程
1，生产者客户端应用程序产生消息
2，客户端连接对象将消息包装到请求中，发送到服务端
3，服务端连接对象负责接收请求，并将消息以文件形式存储
4，服务端返回响应结果给生产者客户端

#### 首先我们来看下kafka发送消息的代码
```java
public class Producer extends Thread {
    private final KafkaProducer<Integer, String> producer;
    private final String topic;

    public Producer(String topic, Boolean isAsync) {
        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put("client.id", "DemoProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
        this.topic = topic;
    }

    public void run() {
        int messageNo = 1;
        while (true) {
            String messageStr = "Message_" + messageNo;
            try {
                producer.send(new ProducerRecord<>(topic,
                        messageNo,
                        messageStr)).get();
                System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            ++messageNo;
        }
    }
}
```

生产者客户端对象KafkaProducer的send方法的处理逻辑是：首先序列化消息的key和value（消息必须序列化成二进制流的形式才能在网络中传输），然后为每一条消息息选择对应的分区（表示要将消息存储至Kafka集群的哪个节点上），最后通知发送线程发送消息。所以大家可以明白kafka使用独立的线程发送消息。
那我们来看下send方法，底层调用的是doSend方法
```java
private Future<RecordMetadata> doSend(ProducerRecord<K, V> record, Callback callback) {
    TopicPartition tp = null;
    try {
        //...省略代码
        //序列化消息key
        byte[] serializedKey;
        try {
            serializedKey = keySerializer.serialize(record.topic(), record.headers(), record.key());
        } catch (ClassCastException cce) {
        }
        //序列化消息value
        byte[] serializedValue;
        try {
            serializedValue = valueSerializer.serialize(record.topic(), record.headers(), record.value());
        } catch (ClassCastException cce) {
        }
        //选择消息发送到哪个分区，并构造TopicPartition
        int partition = partition(record, serializedKey, serializedValue, cluster);
        tp = new TopicPartition(record.topic(), partition);
        //...省略代码
        //将消息和TopicPartition追加到记录收集器
        RecordAccumulator.RecordAppendResult result = accumulator.append(tp, timestamp, serializedKey,
                serializedValue, headers, interceptCallback, remainingWaitMs);
        //如果记录收集器消息满了，则通知Sender线程发送消息
        if (result.batchIsFull || result.newBatchCreated) {
            this.sender.wakeup();
        }
        return result.future;
    } catch (Exception e) {
      //...省略代码
    }
}
```
从以上代码中，我们可以得知kafka消息发送到kafka的哪个分区，是在客户端发送的时候就决定好了，这样的一种思想是依赖倒置，集群中数据分布不是由kafka集群来决定，而是由客户端来决定，这样的好处是消息负载均衡方式更加灵活，而且broker端也更简单，只需要把消息直接存储就行，不需要考虑数据该如何负载均衡。

那么问题来，kafka客户端如何为消息选择分区的呢？
```java
private int partition(ProducerRecord<K, V> record, byte[] serializedKey, byte[] serializedValue, Cluster cluster) {
    Integer partition = record.partition();
    return partition != null ?
            partition :
            partitioner.partition(
                    record.topic(), record.key(), serializedKey, record.value(), serializedValue, cluster);
}
```
这里是由分区器来对消息进行分区，kafka客户端提供了默认的分区器
```java
public class DefaultPartitioner implements Partitioner {

    private final ConcurrentMap<String, AtomicInteger> topicCounterMap = new ConcurrentHashMap<>();
    public void configure(Map<String, ?> configs) {}
    //为消息选择分区编号
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //获取topic的所有分区，用来实现消息的负载均衡
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if (keyBytes == null) {//未设置key，则均匀分布，roundrobin
            int nextValue = nextValue(topic);
            //筛选出可用的分区
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
            if (availablePartitions.size() > 0) {
                int part = Utils.toPositive(nextValue) % availablePartitions.size();
                return availablePartitions.get(part).partition();
            } else {
                return Utils.toPositive(nextValue) % numPartitions;
            }
        } else {
            //其中，这里的org.apache.kafka.common.utils.Utils#murmur2是将key的字节转hash成数字的工具，用来计算消息分区
            return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        }
    }

    private int nextValue(String topic) {
        AtomicInteger counter = topicCounterMap.get(topic);
        if (null == counter) {
            counter = new AtomicInteger(ThreadLocalRandom.current().nextInt());
            AtomicInteger currentCounter = topicCounterMap.putIfAbsent(topic, counter);
            if (currentCounter != null) {
                counter = currentCounter;
            }
        }
        return counter.getAndIncrement();
    }

    public void close() {}

}
```


