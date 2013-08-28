1.使用方法：
import com.renren.kafka.client.ProducerClient;
//获取ProducerClient 实例
ProducerClient producerClient = ProducerClient.getInstance();
//初始化，使用配置文件producer.properties
producerClient.init("producer.properties");

//调用send 方法发送数据
producerClient.sendProduceDataById(topic,id % partition, value);

topic 表示数据的话题，partition 表示partition 个数，value是字符串


2. demo程序发送JsonObject, 运行
  java KafkaClient  topicName parititonNum  propertiesFile logConfFile dataFile


