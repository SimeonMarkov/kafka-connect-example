package com.kafka.connect.file;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceTask;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileStreamSourceConnector extends SourceConnector {

    public static final String TOPIC_CONFIG = "topic";
    public static final String FILE_CONFIG = "file";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FILE_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "Source filename. If not specified, the standard input will be used")
            .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "The topic to publish data to");

    private String filename;
    private String topic;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        filename = props.get(FILE_CONFIG);
        topic = props.get(TOPIC_CONFIG);
        if (topic == null || topic.isEmpty())
            throw new ConnectException("FileStreamSourceConnector configuration must include 'topic' setting");
        if (topic.contains(","))
            throw new ConnectException("FileStreamSourceConnector should only have a single topic when used as a source.");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FileStreamSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        // Only one input stream makes sense.
        Map<String, String> config = new HashMap<>();
        if (filename != null)
            config.put(FILE_CONFIG, filename);
        config.put(TOPIC_CONFIG, topic);
        configs.add(config);
        return configs;
    }

    @Override
    public void stop() {

    }

    public static ConfigDef getConfigDef() {
        return CONFIG_DEF;
    }
}
