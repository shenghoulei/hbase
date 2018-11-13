package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * Hbase管理类,相当于工厂类
 *
 * @author jianting.zhao
 */
public class HBaseConnectionUtil {
	private static Configuration conf;
	private static Connection conn;

	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03,hadoop04,hadoop05");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.master.port", "60000");
	}

	/**
	 * 获取到hbase的连接
	 *
	 * @return 返回连接
	 */
	public static synchronized Connection getConnection() {
		// conn 不为空，则直接返回
		if (null != conn) {
			return conn;
		}

		// conn 为空，则创建后返回
		try {
			conn = ConnectionFactory.createConnection(conf);
			return conn;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

	}

	/**
	 * 关闭连接，释放资源
	 */
	public static synchronized void closeConnection() {
		// conn为空，则直接结束
		if (null == conn) {
			return;
		}

		// conn不为空，则关闭连接后结束
		try {
			conn.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}

