package P2PTest;

import com.rabbitmq.client.*;
import org.junit.Test;

import java.io.IOException;

public class P2PTest {
    @Test
    public void send() throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setVirtualHost("/");
        factory.setUsername("guest");
        factory.setPassword("guest");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare("queue_name",false,false,false,null);
        String message = "hello";
        channel.basicPublish("","queue_name",null,message.getBytes());
        channel.close();
        connection.close();
    }

    @Test
    public void receive() throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setVirtualHost("/");
        factory.setUsername("guest");
        factory.setPassword("guest");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare("queue_name",false,false,false,null);

        channel.basicConsume("queue_name",new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag,Envelope envelope,AMQP.BasicProperties properties, byte[] body) throws IOException {
              String message = new String(body, "UTF-8");
              System.out.println(" 接收到的消息是： '" + message + "'");
            }
        });

    }
}
