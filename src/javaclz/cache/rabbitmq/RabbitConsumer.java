package javaclz.cache.rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by xiaoke on 17-11-9.
 */
public class RabbitConsumer {

    private final String host;

    private final int port;

    private final String queueName;

    private Connection connection;

    private Channel channel;

    private RabbitCListener listener;

    public RabbitConsumer (String host, int port, String queueName) {
        this.host = host;
        this.port = port;
        this.queueName = queueName;
    }

    public void start(RabbitCListener lis) throws Exception{
        this.listener = lis;
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("admin");
        factory.setPassword("admin");
        factory.setHost(host);
        factory.setVirtualHost("/");
        factory.setPort(port);
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.queueDeclare(queueName, false, false, false, null);
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                if (listener != null) {
                    listener.msgIn(consumerTag, body);
                }

            }
        };
        channel.basicConsume(queueName, true, consumer);
    }

    public void stop() {

        if(channel != null) {
            try {
                channel.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
