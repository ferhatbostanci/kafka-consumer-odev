import java.util.Scanner;

public final class Main {

  public static void main(String[] args) {

    int consumerCount;
    // 1 ila 1000 arasında kaç adet random sayı üretilecek
    int randomNumberCount = 100;

    // Kafka server ayarları burada
    String bootstrapServer = "192.168.4.7:9092";
    String topic = "vize";

    Scanner scan = new Scanner(System.in);
    System.out.print("Lütfen oluşturmak istediğiniz Consumer sayısını giriniz: ");
    consumerCount = scan.nextInt();
    scan.close();

    // İlk önce consumerlerımı oluşturuyorum ve poll nedeniyle produceri bekliyorlar
    ConsumerThread consumerThread = new ConsumerThread(bootstrapServer, topic, randomNumberCount, consumerCount);
    for(int i = 0; i < consumerCount; i++){
      Thread t = new Thread(consumerThread);
      t.start();
    }


    // Son olarak test için Producer thread başlatıyorum
    ProducerThread producerThread = new ProducerThread(bootstrapServer, topic, randomNumberCount);
    Thread p1 = new Thread(producerThread);
    p1.start();

  }

}