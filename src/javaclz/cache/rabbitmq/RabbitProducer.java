package javaclz.cache.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by xiaoke on 17-11-9.
 */
public class RabbitProducer {

    private final String host;

    private final int port;

    private final String queueName;

    private Connection connection;

    private Channel channel;

    public RabbitProducer (String host, int port, String queueName) {
        this.host = host;
        this.port = port;
        this.queueName = queueName;

    }

    public void start() throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("admin");
        factory.setPassword("admin");
        factory.setHost(host);
        factory.setVirtualHost("/");
        factory.setPort(port);
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.queueDeclare(queueName, false, false, false, null);
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

    public boolean publish(String msg) {
        for (int i = 0; i < 3; i++) {
            try {
                channel.basicPublish("", queueName, null, msg.getBytes());
                return true;
            } catch (Exception e) {
                continue;
            }
        }
        return false;
    }

}
