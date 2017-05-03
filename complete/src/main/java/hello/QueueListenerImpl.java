package hello;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueListenerImpl implements QueueListener<DataObject> {

    private static final Logger logger = LoggerFactory.getLogger(QueueListenerImpl.class);

    @Override
    public void onMessage(DataObject object) {
        String message = JsonUtils.marshal(object);
        logger.info("received message : {}", message);
    }
}
