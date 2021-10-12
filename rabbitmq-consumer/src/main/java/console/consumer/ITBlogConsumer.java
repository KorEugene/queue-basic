package console.consumer;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class ITBlogConsumer {

    private static final String EXCHANGE_NAME = "ITBlog";
    private static final String SET_TOPIC = "set_topic";
    private static final String UNSET_TOPIC = "unset_topic";
    private static final String EXIT = "exit";
    private static final Map<String, TopicThread> TOPICS = new HashMap<>();

    public static void main(String[] argv) throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setUsername("admin");
        factory.setPassword("admin");

        Scanner scanner = new Scanner(System.in);

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

            while (true) {
                System.out.print("Enter topic: ");
                String inputData = scanner.nextLine();

                if (inputData.trim().equalsIgnoreCase(EXIT)) {
                    stopSubscribes();
                    break;
                }

                String[] data = inputData.trim().split(" ", 2);

                if (data.length < 2) {
                    continue;
                }

                String command = data[0];
                String topicName = data[1];

                if (command.trim().equalsIgnoreCase(SET_TOPIC)) {
                    subscribeTopic(channel, topicName);
                    System.out.println("Subscribed: " + topicName);
                } else if (command.trim().equalsIgnoreCase(UNSET_TOPIC)) {
                    unsubscribeTopic(topicName);
                    System.out.println("Unsubscribed: " + topicName);
                } else {
                    System.out.println("You entered wrong command!");
                }
            }
        }
    }

    private static void stopSubscribes() {
        List<String> keys = new ArrayList<>(TOPICS.keySet());
        for (String key : keys) {
            unsubscribeTopic(key);
        }
    }

    private static void subscribeTopic(Channel channel, String topicName) throws IOException {
        if (TOPICS.get(topicName) == null) {
            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, EXCHANGE_NAME, topicName);

            System.out.println("Create new TopicThread for " + topicName);
            TopicThread topicThread = new TopicThread(channel, queueName, topicName);
            topicThread.start();
            TOPICS.put(topicName, topicThread);
        }
    }

    private static void unsubscribeTopic(String topicName) {
        if (TOPICS.get(topicName) != null) {
            TOPICS.get(topicName).stopListening();
            TOPICS.remove(topicName);
        }
    }
}
