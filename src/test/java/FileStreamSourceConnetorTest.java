import com.kafka.connect.file.FileStreamSourceConnector;
import com.kafka.connect.file.FileStreamSourceTask;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class FileStreamSourceConnetorTest extends EasyMockSupport{
    private static final String SINGLE_TOPIC = "test";
    private static final String MULTIPLE_TOPICS = "test1,test2";
    private static final String FILENAME = "/somefilename";

    private FileStreamSourceConnector connector;
    private ConnectorContext ctx;
    private Map<String, String> sourceProperties;

    @Before
    public void setup() {
        connector = new FileStreamSourceConnector();
        ctx = createMock(ConnectorContext.class);
        connector.initialize(ctx);

        sourceProperties = new HashMap<>();
        sourceProperties.put(FileStreamSourceConnector.TOPIC_CONFIG, SINGLE_TOPIC);
        sourceProperties.put(FileStreamSourceConnector.FILE_CONFIG, FILENAME);
    }

    @Test
    public void testSourceTasks() {
        replayAll();

        connector.start(sourceProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertEquals(1, taskConfigs.size());
        assertEquals(FILENAME,
                taskConfigs.get(0).get(FileStreamSourceConnector.FILE_CONFIG));
        assertEquals(SINGLE_TOPIC,
                taskConfigs.get(0).get(FileStreamSourceConnector.TOPIC_CONFIG));

        // Should be able to return fewer than requested #
        taskConfigs = connector.taskConfigs(2);
        assertEquals(1, taskConfigs.size());
        assertEquals(FILENAME,
                taskConfigs.get(0).get(FileStreamSourceConnector.FILE_CONFIG));
        assertEquals(SINGLE_TOPIC,
                taskConfigs.get(0).get(FileStreamSourceConnector.TOPIC_CONFIG));

        verifyAll();
    }

    @Test
    public void testSourceTasksStdin() {
        EasyMock.replay(ctx);

        sourceProperties.remove(FileStreamSourceConnector.FILE_CONFIG);
        connector.start(sourceProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertEquals(1, taskConfigs.size());
        assertNull(taskConfigs.get(0).get(FileStreamSourceConnector.FILE_CONFIG));

        EasyMock.verify(ctx);
    }

    @Test(expected = ConnectException.class)
    public void testMultipleSourcesInvalid() {
        sourceProperties.put(FileStreamSourceConnector.TOPIC_CONFIG, MULTIPLE_TOPICS);
        connector.start(sourceProperties);
    }

    @Test
    public void testTaskClass() {
        EasyMock.replay(ctx);

        connector.start(sourceProperties);
        assertEquals(FileStreamSourceTask.class, connector.taskClass());

        EasyMock.verify(ctx);
    }
}
