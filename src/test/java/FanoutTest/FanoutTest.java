package FanoutTest;

import Work.Util.ConnectionUtil;
import com.rabbitmq.client.*;
import org.junit.Test;

import java.io.IOException;

public class FanoutTest {
    @Test
    public void send() throws Exception{
        Connection connection = ConnectionUtil.getConn();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare("fanoutEx","fanout");

        for(int i=0;i<100;i++){
            String msg="hello:" + i +"message";
            channel.basicPublish("","work",null,msg.getBytes());
            System.out.println("第"+i+"条消息已发送");
        }
        channel.close();
        connection.close();
    }

    @Test
    public void receive1() throws Exception {
        Connection connnection = ConnectionUtil.getConn();
        final Channel channel = connnection.createChannel();
        channel.queueDeclare("fanout01", false, false, false, null);
        channel.exchangeDeclare("fanoutEx","fanout");
        channel.queueBind("fanout01","fanoutEx","");
        channel.basicQos(1);

        channel.basicConsume("fanout01",false,new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("一号消费者接收到的消息是： '" + message + "'");
                channel.basicAck(envelope.getDeliveryTag(),false);
            }
        });
    }

    @Test
    public void receive2() throws Exception {
        Connection connnection = ConnectionUtil.getConn();
        final Channel channel = connnection.createChannel();
        channel.queueDeclare("fanout02", false, false, false, null);
        channel.exchangeDeclare("fanoutEx","fanout");
        channel.queueBind("fanout02","fanoutEx","");
        channel.basicQos(1);

        channel.basicConsume("fanout02",false,new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("二号消费者接收到的消息是： '" + message + "'");
                channel.basicAck(envelope.getDeliveryTag(),false);
            }
        });
    }
}
