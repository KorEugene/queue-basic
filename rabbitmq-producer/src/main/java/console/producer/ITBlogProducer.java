package console.producer;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class ITBlogProducer {

    private static final String EXCHANGE_NAME = "ITBlog";
    private static final String EXIT = "exit";

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
                System.out.print("Publish article: ");
                String inputData = scanner.nextLine().toLowerCase();

                if (inputData.trim().equalsIgnoreCase(EXIT)) {
                    break;
                }

                String[] data = inputData.trim().split(" ", 2);

                if (data.length < 2) {
                    continue;
                }

                String routingKey = data[0];
                String message = data[1];

                channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes(StandardCharsets.UTF_8));
                System.out.println("ARTICLE: " + routingKey + " " + message + " SEND");
            }
        }
    }
}
