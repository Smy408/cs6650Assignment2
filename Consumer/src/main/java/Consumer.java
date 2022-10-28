import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;

public class Consumer {

  private final static String QUEUE_NAME = "ride";
  private final static int THREAD_POOL_SIZE = 3;
  private static final String IP = "35.91.164.14";
  private static final int PORT = 5672;
  private static final String USER = "username";
  private static final String PASSWORD = "password";


  public static void main(String[] args) throws IOException, TimeoutException {
    Gson gson = new Gson();
    final Map<Integer, ConcurrentLinkedQueue<Integer>> map =
        new ConcurrentHashMap<>();
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(IP);
    factory.setPort(PORT);
    factory.setUsername(USER);
    factory.setPassword(PASSWORD);
    Connection connection = factory.newConnection();

    Runnable runnable = () -> {
      try {
        final Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(
            " [*] Thread " + Thread.currentThread().getId() + "awaiting requests");

        final DeliverCallback callback =
            (consumerTag, delivery) -> {
              String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
              JsonObject body = gson.fromJson(message, JsonObject.class);
              Integer skierID = body.get("skierID").getAsInt();
              Integer liftID = body.get("liftID").getAsInt();
//              System.out.println(String.format("skierID: %d, liftID: %d", skierID, liftID));
              ConcurrentLinkedQueue<Integer> queue = map.getOrDefault(skierID,
                  new ConcurrentLinkedQueue<>());
              queue.add(liftID);
              map.put(skierID, queue);
              channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            };
        channel.basicConsume(QUEUE_NAME, false, callback, consumerTag -> {
        });
      } catch (IOException e) {
        e.printStackTrace();
      }
    };

    for (int i = 0; i < THREAD_POOL_SIZE; i++) {
      Thread thread = new Thread(runnable);
      thread.start();
    }

    System.out.println("[x] Connection is ready, " + THREAD_POOL_SIZE +
        " Thread waiting for messages. To exit press CTRL+C\"");
  }
}
