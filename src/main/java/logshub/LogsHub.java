package logshub;

import io.github.cdimascio.dotenv.Dotenv;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import com.google.gson.Gson;
import java.util.Dictionary;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class LogsHub {

   private Producer<String, String> producer;

   public LogsHub(){
      Properties producerProperties = initProducerProperties();
      this.producer = new KafkaProducer<>(producerProperties);
   }

   public void pushIntoKafka(String record  ){

      String kafka_topic = getConfigValue("TOPIC");
      ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(kafka_topic, record);
      producer.send(producerRecord);
      producer.flush();
   }

   private Properties initProducerProperties() {
      Properties producerProperties = new Properties();

      producerProperties.put("block.on.buffer.full", true);
      producerProperties.put("buffer.memory", 134217728);
      producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getConfigValue("KAFKA_BROKERS"));
      producerProperties.put(ProducerConfig.ACKS_CONFIG, getConfigValue("kafka_acks"));
      producerProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, getConfigValue("kafka_compression_type"));
      producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
      producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
      producerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, getConfigValue("kafka_batch_size"));
      producerProperties.put(ProducerConfig.LINGER_MS_CONFIG,getConfigValue("kafka_linger_ms") );
      producerProperties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,getConfigValue("kafka_max_request_size"));
      producerProperties.put(ProducerConfig.RETRIES_CONFIG,  getConfigValue("kafka_retries") );

      return  producerProperties ;
   }


   private String getConfigValue(String KeyName){
      Dotenv dotenv = Dotenv.load();
      return dotenv.get(KeyName);
   }

   private String _log(String msg, String application, String level, String execution_time, String user_id, Dictionary extra_data) {

      String environment = getConfigValue("ENVIRONMENT");

      if (environment.length() ==0 )
            environment = "development";

      if ( application.length() == 0 )
          application = getConfigValue("APPLICATION") ;

      Long ts = new Date().getTime();
      Gson gson = new Gson();

      String xtra_data = gson.toJson(extra_data);

      Map<String, String> my_dict = new HashMap<String, String>();
      my_dict.put("message", msg);
      my_dict.put("level", level);
      my_dict.put("application", application );
      my_dict.put("environment", environment);
      my_dict.put("user_id", user_id);
      my_dict.put("execution_time", execution_time);
      my_dict.put("extra_data", xtra_data);
      my_dict.put("timestamp", ts.toString());

      String final_json = gson.toJson(my_dict);
      return final_json;
   }


   public  String info(String msg, String application,String execution_time,String user_id,Dictionary extra_data){
      String level = getConfigValue("INFO");
      String data =  _log(msg,application,level,execution_time,user_id,extra_data );
      pushIntoKafka(data);
      return data;
   }

   public  String error(String msg, String application,String execution_time,String user_id,Dictionary extra_data){
      String level = getConfigValue("ERROR");
      String data =  _log(msg,application,level,execution_time,user_id,extra_data );
      pushIntoKafka(data);
      return data;
   }

   public  String warning(String msg, String application,String execution_time,String user_id,Dictionary extra_data){
      String level = getConfigValue("WARNING");
      String data =  _log(msg,application,level,execution_time,user_id,extra_data );
      pushIntoKafka(data);
      return data;
   }

   public  String critical(String msg, String application,String execution_time,String user_id,Dictionary extra_data){
      String level = getConfigValue("CRITICAL");
      String data =  _log(msg,application,level,execution_time,user_id,extra_data );
      pushIntoKafka(data);
      return data;
   }

   public  String metrics(String msg, String application,String execution_time,String user_id,Dictionary extra_data){
      String level = getConfigValue("METRIC");
      String data =  _log(msg,application,level,execution_time,user_id,extra_data );
      pushIntoKafka(data);
      return data;
   }

}
