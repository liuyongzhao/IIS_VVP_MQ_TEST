package Work;

import Work.Util.ConnectionUtil;
import com.rabbitmq.client.*;
import org.junit.Test;

import java.io.IOException;

public class WorkTest {
    @Test
    public void send() throws Exception{
        Connection connection = ConnectionUtil.getConn();
        Channel channel = connection.createChannel();
        channel.queueDeclare("work",false,false,false,null);
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
        channel.queueDeclare("work", false, false, false, null);
        channel.basicQos(1);

        channel.basicConsume("work",false,new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" 接收到的消息是： '" + message + "'");
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }finally {
                    channel.basicAck(envelope.getDeliveryTag(),false);
                }
            }
        });
    }

    @Test
    public void receive2() throws Exception {
        Connection connnection = ConnectionUtil.getConn();
        final Channel channel = connnection.createChannel();
        channel.queueDeclare("work", false, false, false, null);
        channel.basicQos(1);

        channel.basicConsume("work",false,new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" 接收到的消息是： '" + message + "'");
                try {
                    Thread.sleep(150);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }finally {
                    channel.basicAck(envelope.getDeliveryTag(),false);
                }
            }
        });
    }

}
