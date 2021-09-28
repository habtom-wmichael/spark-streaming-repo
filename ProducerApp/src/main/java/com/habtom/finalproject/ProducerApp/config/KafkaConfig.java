/**
 * 
 */
package com.habtom.finalproject.ProducerApp.config;

/**
 * @author cloudera
 *
 */
public class KafkaConfig {

	public static final String KAFKA_BROKERS = "localhost:9092";
    public static final String TOPIC = "tweets";
    public static final long SLEEP_TIMER = 1000;
    
   // public static String CLIENT_ID="client1";    
    public static String GROUP_ID_CONFIG="G1";
    
    public static String OFFSET_RESET_LATEST="latest";
   // public static String OFFSET_RESET_EARLIER="earliest";
    
    public static Integer MAX_POLL_RECORDS=1;
    public static Integer MESSAGE_SIZE=100000000;
    public static Integer MESSAGE_COUNT=1000;
    public static Integer MAX_NO_MESSAGE_FOUND_COUNT=100;
}
