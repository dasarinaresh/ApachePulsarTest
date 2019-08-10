/**
 * @author NareshDasari
 * @created Aug 10, 2019
 */

package org.naresh.learn.apachepulsar;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public class PulsarConsumer {

	public static void main(String[] args) throws PulsarClientException {

		PulsarClient client = PulsarClient.builder()
                						.serviceUrl("pulsar://localhost:6650")
                						.build();
		
		Consumer consumer = client.newConsumer()
									.topic("topicNdasa")
									.subscriptionName("testSubscription")
									.subscribe();
		
		while(true) {
			Message msg = consumer.receive();
			System.out.println("Pulsar Consumer >> "+ new String(msg.getData()));
			consumer.acknowledge(msg); 
		}
		
	}
}
