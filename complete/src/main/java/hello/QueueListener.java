package hello;

public interface QueueListener<T> {

    void onMessage(T message);
}
