package com.habtom.finalproject.ProducerApp;

import com.habtom.finalproject.ProducerApp.utlity.KakfaProducerApp;



/**
 * Hello world!
 *
 */
public class ProducerApp 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello Final Project!" );
        System.out.println("*******************2021***********************************");
		System.out.println("KafkaProducerApp is intializing................");
		System.out.println("Starting ....");
		
		
		KakfaProducerApp producer = new KakfaProducerApp();
	        producer.run();
	        System.out.println("TwitterKafkaProducerApp is running");
    }
}
