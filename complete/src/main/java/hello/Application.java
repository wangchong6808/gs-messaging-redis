package hello;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class Application {

	private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

	@Bean
	RedisMessageListenerContainer container(RedisConnectionFactory connectionFactory,
			MessageListenerAdapter listenerAdapter) {

		RedisMessageListenerContainer container = new RedisMessageListenerContainer();
		container.setConnectionFactory(connectionFactory);
		container.addMessageListener(listenerAdapter, new PatternTopic("chat"));

		return container;
	}

	@Bean
	MessageListenerAdapter listenerAdapter(Receiver receiver) {
		return new MessageListenerAdapter(receiver, "receiveMessage");
	}

	@Bean
	Receiver receiver(CountDownLatch latch, StringRedisTemplate redisTemplate) {
		return new Receiver(latch, redisTemplate);
	}

	@Bean
	CountDownLatch latch() {
		return new CountDownLatch(1);
	}

	@Bean
	StringRedisTemplate template(RedisConnectionFactory connectionFactory) {
		return new StringRedisTemplate(connectionFactory);
	}

	public static void main(String[] args) throws InterruptedException {

		ApplicationContext ctx = SpringApplication.run(Application.class, args);

		StringRedisTemplate template = ctx.getBean(StringRedisTemplate.class);
		CountDownLatch latch = ctx.getBean(CountDownLatch.class);
		String queueName = "queue";
		new MessageListenerRegister(template).register(queueName, new QueueListenerImpl());
		LOGGER.info("Sending message...");
		DataObject object = getDataObject();
		template.opsForList().leftPush(queueName, JsonUtils.marshal(object));
		TimeUnit.SECONDS.sleep(4);
		object.setName("Tom");
		template.opsForList().leftPush(queueName, JsonUtils.marshal(object));
		TimeUnit.SECONDS.sleep(2);
		object.setAge(11);
		template.opsForList().leftPush(queueName, JsonUtils.marshal(object));
		TimeUnit.SECONDS.sleep(5);
		object.setPlaying(false);
		template.opsForList().leftPush(queueName, JsonUtils.marshal(object));
		TimeUnit.SECONDS.sleep(2);
		object.setPlaying(true);
		template.opsForList().leftPush(queueName, JsonUtils.marshal(object));
		template.convertAndSend("chat", "Hello from Redis!");
		latch.await();

		System.exit(0);
	}

	private static DataObject getDataObject(){
		DataObject dataObject = new DataObject();
		dataObject.setName("Jack");
		dataObject.setAge(10);
		dataObject.setPlaying(true);
		return dataObject;
	}
}
