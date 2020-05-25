package com.sample.kafka.stream;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

public class WordCountDemo {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());	

		
		final StreamsBuilder builder = new StreamsBuilder();
		Topology build = builder.build();
		KStream<String, String> textStream = builder.stream("input-topic");
		Pattern pattern=Pattern.compile("\\W+",Pattern.UNICODE_CHARACTER_CLASS);
		KTable<String, Long> count = textStream.flatMapValues(value->Arrays.asList(pattern.split(value))).groupBy((key,word)->word).count();
		System.out.println("Describe Builder "+build.describe());
		count.foreach((word,count1)->{
			System.out.println(" Word , Count "+word+" "+count1);
		});;


		
		count.to(Serdes.String(),Serdes.Long(),"outut-topic");

		KafkaStreams streams=new KafkaStreams(build, props);
		final CountDownLatch latch=new CountDownLatch(1);
		
		Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
		    @Override
		    public void run() {
		    	System.out.println("Closing Program");
		        streams.close();
		        latch.countDown();
		    }
		});
		
		try {
			streams.start();
//			latch.await();
			Thread.sleep(1000l);
			streams.close();
		}catch (Throwable e) {
			System.exit(1);
		}
	}

}
