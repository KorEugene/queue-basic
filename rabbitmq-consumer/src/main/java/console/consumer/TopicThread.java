package console.consumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TopicThread extends Thread {

    private final Channel channel;
    private final String queueName;
    private final String topicName;
    private String tag;

    public TopicThread(Channel channel, String queueName, String topicName) {
        this.channel = channel;
        this.queueName = queueName;
        this.topicName = topicName;
    }

    @Override
    public void run() {
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("RECEIVED MESSAGE FROM '" + topicName + "' : '" + message + "'");
        };

        try {
            tag = channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });
        } catch (IOException exception) {
            System.out.println("Error! " + exception.getMessage());
        }
    }

    public void stopListening() {
        try {
            channel.basicCancel(tag);
        } catch (IOException exception) {
            System.out.println("Error! " + exception.getMessage());
        }
    }
}
