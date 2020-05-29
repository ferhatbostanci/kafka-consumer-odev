import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerThread implements Runnable{

    protected String bootstrapServer;
    protected String topic;
    protected int randomNumberCount;

    public ProducerThread(String bootstrapServer, String topic, int randomNumberCount){
        this.bootstrapServer = bootstrapServer;
        this.topic = topic;
        this.randomNumberCount = randomNumberCount;
    }

    @Override
    public void run(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        try{
            for(int i = 1; i <= randomNumberCount; i++){
                Random rand = new Random();
                String value = Integer.toString(rand.nextInt(1000));
                producer.send(new ProducerRecord<String, String>(this.topic, value)).get();
            }
        }catch(Exception e){
            e.printStackTrace();
        }

        producer.flush();
        producer.close();
        Thread.currentThread().stop();
    }

}
