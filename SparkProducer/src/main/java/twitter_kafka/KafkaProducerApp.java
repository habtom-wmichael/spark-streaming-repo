package twitter_kafka;

public class KafkaProducerApp {

	public static void main(String[] args) {
		System.out.println("*******************2021***********************************");
		System.out.println("KafkaProducerApp is intializing................");
		System.out.println("Starting ....");
		
		
		 TwitterKafkaProducer producer = new TwitterKafkaProducer();
	        producer.run();
	        System.out.println("TwitterKafkaProducerApp is running");

	}

}
