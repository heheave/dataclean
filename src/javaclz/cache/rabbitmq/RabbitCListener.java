package javaclz.cache.rabbitmq;

/**
 * Created by xiaoke on 17-11-9.
 */
public interface RabbitCListener {
    void msgIn(String ctag, byte[] body);
}
