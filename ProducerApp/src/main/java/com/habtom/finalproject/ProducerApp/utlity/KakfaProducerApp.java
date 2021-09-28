/**
 * 
 */
package com.habtom.finalproject.ProducerApp.utlity;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;





import com.google.gson.Gson;
import com.habtom.finalproject.ProducerApp.config.CallBacker;
import com.habtom.finalproject.ProducerApp.config.KafkaConfig;
import com.habtom.finalproject.ProducerApp.config.TwitterConfig;
import com.habtom.finalproject.ProducerApp.pojo.Tweet;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

/**
 * @author cloudera
 *
 */
public class KakfaProducerApp {
	

	private Client client;
	private BlockingQueue<String> queue;
	private Gson gson;
	private CallBacker callback;

	public KakfaProducerApp() {
		
		Authentication authentication = new OAuth1(
				TwitterConfig.CONSUMER_KEY,
				TwitterConfig.CONSUMER_SECRET,
				TwitterConfig.ACCESS_TOKEN,
				TwitterConfig.ACCESS_TOKEN_SECRET);

		// track the terms of your choice. here i'm only tracking #bigdata.
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		endpoint.trackTerms(Arrays.asList(TwitterConfig.HASHTAG
				.split(",")));

		StatusesFilterEndpoint endpointLocation = new StatusesFilterEndpoint(
				false).locations(Arrays.asList(new Location(
				new Location.Coordinate(19.50139, -161.75583),
				new Location.Coordinate(64.85694, -68.01197))));
		queue = new LinkedBlockingQueue<String>(10000);

		client = new ClientBuilder().hosts(Constants.STREAM_HOST)
				.authentication(authentication).endpoint(endpointLocation)
				.endpoint(endpoint)
				.processor(new StringDelimitedProcessor(queue)).build();
		gson = new Gson();
		callback = new CallBacker();
	}

	private Producer<Long, String> getProducer() {
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
				KafkaConfig.KAFKA_BROKERS);
		properties.put(ProducerConfig.ACKS_CONFIG, "1");
		properties.put(ProducerConfig.LINGER_MS_CONFIG, 500);
		properties.put(ProducerConfig.RETRIES_CONFIG, 0);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				LongSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());

		return new KafkaProducer<Long, String>(properties);
	}

	public void run() {
		client.connect();
		try (Producer<Long, String> producer = getProducer()) {
			while (true) {
				Tweet tweet = gson.fromJson(queue.take(), Tweet.class);
				if (tweet.getUser() != null
						&& tweet.getUser().getLocation() != null
						&& tweet.getUser().getLocation().contains("USA")) {
					System.out.printf("Twitter is Tweeting at id:- %d\n", tweet.getId());
					long key = tweet.getId();
					System.out.println("tweet " + "   :   " + tweet.toString());
					ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(
							KafkaConfig.TOPIC, key, tweet.toString());
					producer.send(record, callback);
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			client.stop();
		}
	}
}


