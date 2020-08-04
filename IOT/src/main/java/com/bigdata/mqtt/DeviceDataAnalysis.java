package com.bigdata.mqtt;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;

import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class DeviceDataAnalysis {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		Properties properties = new Properties();
		properties.load(new FileInputStream("src/main/resources/application.properties"));

		Properties mqttProperties = new Properties();

		// client id = a:<Organization_ID>:<App_Id>
		mqttProperties.setProperty(MQTTSource.CLIENT_ID,
				String.format("a:%s:%s",
						properties.getProperty("Org_ID"),
						properties.getProperty("App_Id")));

		// mqtt server url = tcp://<Org_ID>.messaging.internetofthings.ibmcloud.com:1883
		mqttProperties.setProperty(MQTTSource.URL,
				String.format("tcp://%s.messaging.internetofthings.ibmcloud.com:1883",
						properties.getProperty("Org_ID")));

		// topic = iot-2/type/<Device_Type>/id/<Device_ID>/evt/<Event_Id>/fmt/json
		mqttProperties.setProperty(MQTTSource.TOPIC,
				String.format("iot-2/type/%s/id/%s/evt/%s/fmt/json",
						properties.getProperty("Device_Type"),
						properties.getProperty("Device_ID"),
						properties.getProperty("EVENT_ID")));

		mqttProperties.setProperty(MQTTSource.USERNAME, properties.getProperty("API_Key"));
		mqttProperties.setProperty(MQTTSource.PASSWORD, properties.getProperty("APP_Authentication_Token"));


		MQTTSource mqttSource = new MQTTSource(mqttProperties);
		DataStreamSource<String> tempratureDataSource = env.addSource(mqttSource);
		DataStream<String> stream = tempratureDataSource.map((MapFunction<String, String>) s -> s);
		stream.print();

		env.execute("Temperature Analysis");
	}

	private static class MQTTSource implements SourceFunction<String> {

		// ----- Required property keys
		public static final String URL = "mqtt.server.url";
		public static final String CLIENT_ID = "mqtt.client.id";
		public static final String TOPIC = "mqtt.topic";

		// ------ Optional property keys
		public static final String USERNAME = "mqtt.username";
		public static final String PASSWORD = "mqtt.password";


		private final Properties properties;

		// ------ Runtime fields
		private transient MqttClient client;
		private transient volatile boolean running;
		private transient Object waitLock;

		public MQTTSource(Properties properties) {
			checkProperty(properties, URL);
			checkProperty(properties, CLIENT_ID);
			checkProperty(properties, TOPIC);

			this.properties = properties;
		}

		private static void checkProperty(Properties p, String key) {
			if (!p.containsKey(key)) {
				throw new IllegalArgumentException("Required property '" + key + "' not set.");
			}
		}

		@Override
		public void run(final SourceContext<String> ctx) throws Exception {
			MqttConnectOptions connectOptions = new MqttConnectOptions();
			connectOptions.setCleanSession(true);

			if (properties.containsKey(USERNAME)) {
				connectOptions.setUserName(properties.getProperty(USERNAME));
			}

			if (properties.containsKey(PASSWORD)) {
				connectOptions.setPassword(properties.getProperty(PASSWORD).toCharArray());
			}

			connectOptions.setAutomaticReconnect(true);

			client = new MqttClient(properties.getProperty(URL), properties.getProperty(CLIENT_ID));
			client.connect(connectOptions);

			client.subscribe(properties.getProperty(MQTTSource.TOPIC), (topic, message) -> {
				String msg = new String(message.getPayload(), StandardCharsets.UTF_8);
				ctx.collect(msg);
			});

			//在flink的source多并行度的情况会不会消费小相同的数据
			running = true;
			waitLock = new Object();

			while (running) {
				synchronized (waitLock) {
					waitLock.wait(100L);
				}

			}
		}

		@Override
		public void cancel() {
			close();
		}

		private void close() {
			try {
				if (client != null) {
					client.disconnect();
				}
			} catch (MqttException exception) {

			} finally {
				this.running = false;
			}

			// leave main method
			synchronized (waitLock) {
				waitLock.notify();
			}
		}

	}
}
