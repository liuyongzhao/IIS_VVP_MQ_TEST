package RoutingTest;

import Work.Util.ConnectionUtil;
import com.rabbitmq.client.*;
import org.junit.Test;

import java.io.IOException;

public class RoutingTest {
    @Test
    public void routingSend() throws Exception{
        Connection connection = ConnectionUtil.getConn();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare("directEx","direct");
        String msg = "路由消息";
        channel.basicPublish("directEx","jtt",null,msg.getBytes());
        channel.close();
        connection.close();
    }

    @Test
    public void routingRec01() throws Exception{
        System.out.println("一号消费者等待接收消息");
        Connection conn = ConnectionUtil.getConn();
        final Channel chan = conn.createChannel();
        chan.queueDeclare("direct01", false, false, false, null);
        chan.exchangeDeclare("directEx", "direct");
        chan.queueBind("direct01", "directEx", "jtt");
        chan.basicQos(1);

        chan.basicConsume("direct01",false, new DefaultConsumer(chan){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("一号消费者接收到的消息是： '" + message + "'");
                chan.basicAck(envelope.getDeliveryTag(),false);
            }
        });
    }
    @Test
    public void routingRec02() throws Exception {
        System.out.println("二号消费者等待接收消息");
        Connection conn = ConnectionUtil.getConn();
        final Channel chan = conn.createChannel();
        chan.queueDeclare("direct01", false, false, false, null);
        chan.exchangeDeclare("directEx", "direct");
        chan.queueBind("direct01", "directEx", "jtt0");
        chan.basicQos(1);

        chan.basicConsume("direct01", false, new DefaultConsumer(chan) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("二号消费者接收到的消息是： '" + message + "'");
                chan.basicAck(envelope.getDeliveryTag(), false);
            }
        });
    }

}
