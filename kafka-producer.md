### kafka如何发送消息的呢

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
默认分区器实现了分区器接口，主要分区算法是在partion方法中实现的，参数中除了key，还有对应的value参数，也就是说我们也可以通过消息的value来指定消息发到哪个分区
```java
public interface Partitioner extends Configurable, Closeable {

    /**
     * Compute the partition for the given record.
     *
     * @param topic The topic name
     * @param key The key to partition on (or null if no key)
     * @param keyBytes The serialized key to partition on( or null if no key)
     * @param value The value to partition on or null
     * @param valueBytes The serialized value to partition on or null
     * @param cluster The current cluster metadata
     */
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster);

    /**
     * This is called when partitioner is closed.
     */
    public void close();

}
```
那么，kafka生产者如何对分区选择算法进行定制呢？
其实很简单：
1，实现接口 org.apache.kafka.clients.producer.Partitioner
2，在创建KafkaProducer的时候指定分区器
```java
props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, MockPartitioner.class.getName());
```

然后，我们接着来看消息发送消息的过程，调用了org.apache.kafka.clients.producer.internals.RecordAccumulator#append
```java
public RecordAppendResult append(TopicPartition tp,
                                 long timestamp,
                                 byte[] key,
                                 byte[] value,
                                 Header[] headers,
                                 Callback callback,
                                 long maxTimeToBlock) throws InterruptedException {
    ByteBuffer buffer = null;
    try {
        //从发送队列中找到对应分区的发送ProducerBatch队列
        Deque<ProducerBatch> dq = getOrCreateDeque(tp);
        synchronized (dq) {
            //将当前消息append到最后一个ProducerBatch的消息队列
            RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callback, dq);
            if (appendResult != null)
                return appendResult;
        }
        //省略部分代码...
    } finally { 
        //省略部分代码...
    }
}
```
好，这里我们看下tryAppend做了哪些事情
```java
private RecordAppendResult tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers,
                                     Callback callback, Deque<ProducerBatch> deque) {
    ProducerBatch last = deque.peekLast();
    if (last != null) {
        FutureRecordMetadata future = last.tryAppend(timestamp, key, value, headers, callback, time.milliseconds());
        if (future == null)
            last.closeForRecordAppends();
        else
            return new RecordAppendResult(future, deque.size() > 1 || last.isFull(), false);
    }
    return null;
}

public FutureRecordMetadata tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers, Callback callback, long now) {
    if (!recordsBuilder.hasRoomFor(timestamp, key, value, headers)) {
        return null;
    } else {
        Long checksum = this.recordsBuilder.append(timestamp, key, value, headers);
        //省略部分代码...
        FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount,
                                                               timestamp, checksum,
                                                               key == null ? -1 : key.length,
                                                               value == null ? -1 : value.length);
        //省略部分代码...
        return future;
    }
}

//recordsBuilder.append最后调用的是appendDefaultRecord方法
private void appendDefaultRecord(long offset, long timestamp, ByteBuffer key, ByteBuffer value,
                                 Header[] headers) throws IOException {
    //省略部分代码...
    int sizeInBytes = DefaultRecord.writeTo(appendStream, offsetDelta, timestampDelta, key, value, headers);
    //省略部分代码...
}
```
最终，这个消息的会被写入到appendStream里面，然后就返回了，那么谁来发送这个DataOutputStream中的消息数据呢？
这里，我们想起来在创建KafkaProducer的时候，里面有个Sender类型的成员org.apache.kafka.clients.producer.internals.Sender，这个Sender实现了Runnable接口，那么我看下run()方法做了哪些事情呢？
```java
public void run() {
    while (running) {
        try {
            run(time.milliseconds());
        } catch (Exception e) {
            log.error("Uncaught error in kafka producer I/O thread: ", e);
        }
    }
    //省略部分代码...
}
//这里run方法实际调用了另外一个run方法
void run(long now) {
    //省略部分代码...
    long pollTimeout = sendProducerData(now);
    client.poll(pollTimeout, now);
}

private long sendProducerData(long now) {
    Cluster cluster = metadata.fetch();
    //省略部分代码...
    RecordAccumulator.ReadyCheckResult result = this.accumulator.ready(cluster, now);
    //省略部分代码...
    Map<Integer, List<ProducerBatch>> batches = this.accumulator.drain(cluster, result.readyNodes,
            this.maxRequestSize, now);
    //省略部分代码...
    sendProduceRequests(batches, now);
    //省略部分代码...
    return pollTimeout;
}
```
sendProducerData方法里面先从元数据中获取当前集群信息，然后在消息聚合器RecordAccumulator中筛选出准备就绪（有些节点可能展示不可用），可发送的节点Broker，然后在消息聚合器RecordAccumulator筛选出节点对应的ProducerBatch队列
接下来，我们来看下真正发送消息sendProduceRequests这部分，实际上调用的是另外一个方法sendProduceRequest
```java
private void sendProduceRequest(long now, int destination, short acks, int timeout, List<ProducerBatch> batches) {
    //省略部分代码...
    Map<TopicPartition, MemoryRecords> produceRecordsByPartition = new HashMap<>(batches.size());
    final Map<TopicPartition, ProducerBatch> recordsByPartition = new HashMap<>(batches.size());
    //省略部分代码...
    for (ProducerBatch batch : batches) {
        TopicPartition tp = batch.topicPartition;
        MemoryRecords records = batch.records();
        //省略部分代码...
        produceRecordsByPartition.put(tp, records);
        recordsByPartition.put(tp, batch);
    }
    //省略部分代码...
    ProduceRequest.Builder requestBuilder = ProduceRequest.Builder.forMagic(minUsedMagic, acks, timeout,
            produceRecordsByPartition, transactionalId);
    RequestCompletionHandler callback = new RequestCompletionHandler() {
        public void onComplete(ClientResponse response) {
            handleProduceResponse(response, recordsByPartition, time.milliseconds());
        }
    };
    
    String nodeId = Integer.toString(destination);
    ClientRequest clientRequest = client.newClientRequest(nodeId, requestBuilder, now, acks != 0, callback);
    client.send(clientRequest, now);
}
```
这里主要做了几件事：将ProducerBatch按分区进行分组，构造成一个分区一个MemoryRecords；然后将这些MemoryRecords以及对应的分区信息构造成一个ClientRequest，通过KafkaClient发送出去，所以我们看下KafkaClient的send方法，实际调用的是
```java
private void doSend(ClientRequest clientRequest, boolean isInternalRequest, long now, AbstractRequest request) {
    String nodeId = clientRequest.destination();
    RequestHeader header = clientRequest.makeHeader(request.version());
    //省略部分代码...
    Send send = request.toSend(nodeId, header);
    InFlightRequest inFlightRequest = new InFlightRequest(
            header,
            clientRequest.createdTimeMs(),
            clientRequest.destination(),
            clientRequest.callback(),
            clientRequest.expectResponse(),
            isInternalRequest,
            request,
            send,
            now);
    this.inFlightRequests.add(inFlightRequest);
    selector.send(inFlightRequest.send);
}

public Send toSend(String destination, RequestHeader header) {
    return new NetworkSend(destination, serialize(header));
}
```
这里，创建了一个NetworkSend类，用于发送消息，我们来看下这个NetworkSend类继承了ByteBufferSend，我们来看下ByteBufferSend的结构
```java
public class ByteBufferSend implements Send {

    private final String destination;
    private final int size;
    protected final ByteBuffer[] buffers;
    private int remaining;
    private boolean pending = false;

    public ByteBufferSend(String destination, ByteBuffer... buffers) {
        this.destination = destination;
        this.buffers = buffers;
        for (ByteBuffer buffer : buffers)
            remaining += buffer.remaining();
        this.size = remaining;
    }

    @Override
    public String destination() {
        return destination;
    }

    @Override
    public boolean completed() {
        return remaining <= 0 && !pending;
    }

    @Override
    public long size() {
        return this.size;
    }

    @Override
    public long writeTo(GatheringByteChannel channel) throws IOException {
        long written = channel.write(buffers);
        if (written < 0)
            throw new EOFException("Wrote negative bytes to channel. This shouldn't happen.");
        remaining -= written;
        pending = TransportLayers.hasPendingWrites(channel);
        return written;
    }
}
```
我们发现这个ByteBufferSend有个writeTo方法，这个方法就是把实际的消息写入Channel发送到远程

另外，在前面方法发送那里，返回给调用方的是一个Future对象方法，大家可以通过那个future获取到实际的执行结果




