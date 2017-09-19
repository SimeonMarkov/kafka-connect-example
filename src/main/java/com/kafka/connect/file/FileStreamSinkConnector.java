package com.kafka.connect.file;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileStreamSinkConnector extends SinkConnector {
    public static final String FILE_CONFIG = "file";
    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FILE_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "Destination filename. If not specified, the standard output will be used");

    private String filename;

    public String version() {
        return AppInfoParser.getVersion();
    }

    public void start(Map<String, String> props) {
        filename = props.get(FILE_CONFIG);
    }

    public Class<? extends Task> taskClass() {
        return FileStreamSinkTask.class;
    }

    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>();
            if (filename != null)
                config.put(FILE_CONFIG, filename);
            configs.add(config);
        }
        return configs;
    }

    public void stop() {

    }

    public static ConfigDef getConfigDef() {
        return CONFIG_DEF;
    }
}
