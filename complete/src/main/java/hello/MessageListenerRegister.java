package hello;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class MessageListenerRegister {

    private static final Logger logger = LoggerFactory.getLogger(MessageListenerRegister.class);
    StringRedisTemplate redisTemplate;
    ThreadPoolTaskExecutor executor;
    public MessageListenerRegister(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
        executor = new ThreadPoolTaskExecutor();
        executor.initialize();
    }

    public void register(String queueName, QueueListener queueListener) {
        executor.execute(() -> {
            while (true) {
                String value = redisTemplate.opsForList().rightPop(queueName, 3, TimeUnit.SECONDS);
                if (value != null) {
                    Class cls = Arrays.asList(queueListener.getClass().getDeclaredMethods()).stream()
                            .filter(method -> method.getName().equals("onMessage")).findFirst().get().getParameterTypes()[0];
                    Object t = JsonUtils.unmarshal(value, cls);
                    queueListener.onMessage(t);
                }else {
                    logger.info("got nothing this time!");
                }
            }
        });
    }
}
