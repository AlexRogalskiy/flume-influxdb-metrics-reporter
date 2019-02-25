package ru.hh.flume.reporting;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.instrumentation.MonitorService;
import org.apache.flume.instrumentation.util.JMXPollUtil;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class InfluxDBReporter implements MonitorService {
    private static final Logger logger = LoggerFactory.getLogger(InfluxDBReporter.class);
    private final InfluxDBCollector collectorRunnable;

    private InfluxDB influxDB;
    private String influxdbUrl = null;
    private String influxdbPrefix = null;
    private String influxdbDatabase = null;
    private String influxdbUsername = null;
    private String influxdbPassword = null;
    private String influxdbRetention = null;
    private Map<String, String> influxdbTags = new HashMap<String,String>();

    private ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

    public final String CONF_URL= "url";
    public final String CONF_TAGS= "tags";
    public final String CONF_PREFIX = "prefix";
    public final String CONF_DATABASE = "database";
    public final String CONF_USERNAME = "username";
    public final String CONF_PASSWORD = "password";
    public final String CONF_RETENTION = "retention";


    public InfluxDBReporter() throws FlumeException {
        collectorRunnable = new InfluxDBCollector();
    }

    public void start() {
        try {
            String result = InetAddress.getLocalHost().getHostName();
            if (StringUtils.isNotEmpty( result)) {
                this.influxdbTags.put("hostname", result);
            }
        } catch (UnknownHostException e) {
            throw new FlumeException("Error getting hostname, " + e);
        }

        try {
            this.influxDB = InfluxDBFactory.connect(
                    influxdbUrl,
                    influxdbUsername,
                    influxdbPassword
            );
        } catch (Exception e) {
            throw new FlumeException("Error creating connection, " + influxdbUrl, e);
        }

        collectorRunnable.reporter = this;
        if (service.isShutdown() || service.isTerminated()) {
            service = Executors.newSingleThreadScheduledExecutor();
        }
        service.scheduleWithFixedDelay(collectorRunnable, 0, 10, TimeUnit.SECONDS);
    }

    public void stop() {
        service.shutdown();

        while (!service.isTerminated()) {
            try {
                logger.warn("Waiting for influxdb reporter to stop");
                service.awaitTermination(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ex) {
                logger.warn("Interrupted while waiting for influxdb monitor to shutdown", ex);
                service.shutdownNow();
            }
        }
    }

    public void configure(Context context) {
        this.influxdbUrl = context.getString(this.CONF_URL, "http://localhost:8086");
        this.influxdbPrefix = context.getString(this.CONF_PREFIX);
        this.influxdbDatabase = context.getString(this.CONF_DATABASE, "flume");
        this.influxdbUsername = context.getString(this.CONF_USERNAME, "flume");
        this.influxdbPassword = context.getString(this.CONF_PASSWORD, "flume");
        this.influxdbRetention = context.getString(this.CONF_RETENTION, "autogen");
        String tags = context.getString(this.CONF_TAGS);
        if(tags != null && !tags.isEmpty()) {
            try {
                for (String tag: tags.split(",")) {
                    String[] kv = tag.split("=");
                    this.influxdbTags.put(kv[0], kv[1]);
                }
            } catch (Exception e) {
                throw new ConfigurationException("Error parsing tags, " + e);
            }
        }
    }

    private void sendPoint(Point point) {
        logger.debug(point.lineProtocol());
        influxDB.write(this.influxdbDatabase, this.influxdbRetention, point);
    }

    protected class InfluxDBCollector implements Runnable {
        private InfluxDBReporter reporter;

        public void run() {
            try {
                Map<String, Map<String, String>> metricsMap = JMXPollUtil.getAllMBeans();
                for (String component : metricsMap.keySet()) {
                    String[] componentParts = component.split("\\.");
                    StringBuilder measurementName = new StringBuilder();

                    if(reporter.influxdbPrefix != null && !reporter.influxdbPrefix.isEmpty()) {
                        measurementName.append(reporter.influxdbPrefix).append("_").append(componentParts[0]);
                    } else {
                        measurementName.append(componentParts[0]);
                    }

                    Point.Builder builder = Point.measurement(measurementName.toString())
                            .tag("name", componentParts[1])
                            .tag(reporter.influxdbTags)
                            .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);

                    Map<String, String> attributeMap = metricsMap.get(component);
                    for (String attribute : attributeMap.keySet()) {
                        try {
                            builder.addField(attribute, Long.parseLong(attributeMap.get(attribute)));
                        } catch (NumberFormatException e) {
                            builder.addField(attribute, attributeMap.get(attribute));
                        }
                    }

                    Point point = builder.build();
                    reporter.sendPoint(point);
                }
            } catch (Throwable t) {
                logger.error("Unexpected error", t);
            }
        }
    }
}

