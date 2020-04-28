package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

public class KProducer {

    private  static String kafkaBrokerEndPoint="http://127.0.0.1:9092";
    private static String topic = "kafka-storm";
    private static String csvPath = "S:\\keywords.csv";

    public static void main(String args[]) throws IOException, InterruptedException {
        //kafkaBrokerEndPoint = args[0];
        //csvPath = args[1];
        //topic=args[2];
        Properties properties =new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaBrokerEndPoint);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        Producer<String,String> csvProd= new KafkaProducer<String, String>(properties);
        BufferedReader br = new BufferedReader(new FileReader(csvPath));
        String line ;
        while((line = br.readLine())!=null && !line.isEmpty()){
            final ProducerRecord<String, String> csvRecord = new ProducerRecord<String, String>(
                    topic, UUID.randomUUID().toString(),line
            );
            csvProd.send(csvRecord);
            Thread.sleep(700);
//            csvProducer.send(csvRecord,(m, ex) -> {
//                if(m != null){
//                    System.out.println("CsvData: -> "+ csvRecord.key()+" | "+ csvRecord.value());
//                }
//                else{
//                    System.out.println("Error Sending Csv Record -> "+ csvRecord.value());
//                }
//            });
        }

    }
}
