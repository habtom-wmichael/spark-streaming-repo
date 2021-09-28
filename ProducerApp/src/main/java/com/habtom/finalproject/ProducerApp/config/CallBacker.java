/**
 * 
 */
package com.habtom.finalproject.ProducerApp.config;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author cloudera
 *
 */
public class CallBacker implements Callback {

	@Override
	public void onCompletion(RecordMetadata arg0, Exception arg1) {


    if (arg1 == null) {
        System.out.printf("Message: %d acknowledged -> on  partition: %d\n",
        		arg0.offset(), arg0.partition());
    } else {
        System.out.println(arg1.getMessage());
    }
}
    
	}
	
	