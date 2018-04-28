package RPCTest;

import Work.Util.ConnectionUtil;
import com.rabbitmq.client.*;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class RPCTest {
    @Test
    public void send() throws Exception{
        Connection connection = ConnectionUtil.getConn();
        final Channel channel = connection.createChannel();
        //声明队列
        channel.queueDeclare("rpc_test", false, false, false, null);

        //限制：每次最多给一个消费者发送1条消息
        channel.basicQos(1);

        //为rpc_queue队列创建消费者，用于处理请求
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                        .Builder()
                        .correlationId(properties.getCorrelationId())
                        .build();
                System.out.println("请求消息的关联ID：" + properties.getCorrelationId());
                System.out.println("发送消息的关联ID：" + replyProps.getCorrelationId());
                System.out.println("接收的消息是：" + body.toString());
                String message = new String(body, "UTF-8");
                String response = "[send]"+message;
                channel.basicPublish("", properties.getReplyTo(), replyProps, response.getBytes("UTF-8"));
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };
        //打开应答机制autoAck=false
        channel.basicConsume("rpc_test", false, consumer);
    }

    @Test
    public void receive() throws Exception {
        Connection connection = ConnectionUtil.getConn();
        final Channel channel = connection.createChannel();
        String replyQueueName = channel.queueDeclare().getQueue();
        String message = "1";
        final String corrId = java.util.UUID.randomUUID().toString();
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .correlationId(corrId).replyTo(replyQueueName).build();
        channel.basicPublish("", "rpc_test", (AMQP.BasicProperties) props,message.toString().getBytes());
        final BlockingQueue<String> response = new ArrayBlockingQueue<String>(1);
        //关闭应答机制，使用自动应答autoAck=true
        channel.basicConsume(replyQueueName, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws
                    IOException {
                if (properties.getCorrelationId().equals(corrId)) {
                    response.offer(new String(body, "UTF-8"));
                }
            }
        });
        channel.close();
        connection.close();
    }
}
