import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerThread implements Runnable {

    public static int allThreadMax = 0;
    public static int recordCount = 0;
    public static int threadCount;

    protected String bootstrapServer;
    protected String topic;
    protected int randomNumberCount;

    public ConsumerThread(String bootstrapServer, String topic, int randomNumberCount, int consumerCount){
        this.bootstrapServer = bootstrapServer;
        this.topic = topic;
        this.randomNumberCount = randomNumberCount;
        threadCount = consumerCount;
    }

    @Override
    public void run(){
        int threadMax = Integer.MIN_VALUE;
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, this.topic);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singletonList(this.topic));

        while(recordCount < randomNumberCount){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for(ConsumerRecord<String, String> record : records){
                int recordValue = (int) Double.parseDouble(record.value());
                if(recordValue > threadMax){
                    threadMax = recordValue;
                }
                checkAllThreadMax(recordValue);
            }
            addRecordCount(records.count());
            consumer.commitSync();
        }
        System.out.println("Consumer Thread ID " + Thread.currentThread().getId() + " | Max Değeri :" + threadMax);
        endMessage();
    }

    // synchronized method oluşturdum, bir thread bu fonksiyonu çağırırsa diğerleri işi bittikten sonra çağırabiliyor bu methodu
    public static synchronized void checkAllThreadMax(int recordValue){
        if(recordValue > allThreadMax){
            allThreadMax = recordValue;
        }
    }

    public static synchronized void addRecordCount(int count){
        recordCount += count;
    }

    public static synchronized void endMessage(){
        threadCount -= 1;
        if(threadCount == 0) System.out.println("\n***\nTüm Thread'ların ortak max değeri: " + allThreadMax + "\n***");
    }

}
