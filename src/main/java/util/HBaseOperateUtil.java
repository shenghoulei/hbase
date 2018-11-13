package util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * HBase工具类，用于HBase上数据的增删改查
 *
 * @author jianting.zhao
 */
public class HBaseOperateUtil {
	private static Connection conn = HBaseConnectionUtil.getConnection();

	/**
	 * 创建表
	 *
	 * @param tableName    表名
	 * @param columFamilys 列簇
	 */
	public static void createTable(String tableName, String... columFamilys) throws Exception {
		if (StringUtils.isBlank(tableName) || columFamilys.length == 0) {
			throw new IllegalArgumentException("表名为空或列族为空");
		}
		if (existsTable(tableName)) {
			throw new Exception("表已经存在");
		}
		HBaseAdmin hAmin = (HBaseAdmin) conn.getAdmin();
		HTableDescriptor hd = new HTableDescriptor(TableName.valueOf(tableName));
		for (String cf : columFamilys) {
			hd.addFamily(new HColumnDescriptor(cf));
		}
		hAmin.createTable(hd);
		hAmin.close();
	}

	/**
	 * 禁用表
	 *
	 * @param table 表名
	 */
	private static void disableTable(String table) throws IOException {
		HBaseAdmin hAmin = (HBaseAdmin) conn.getAdmin();
		hAmin.disableTable(table);
		hAmin.close();
	}

	/**
	 * 删除表
	 *
	 * @param tableName 表名
	 */
	public static void dropTable(String tableName) throws IOException {
		if (!existsTable(tableName)) {
			return;
		}
		HBaseAdmin hAmin = (HBaseAdmin) conn.getAdmin();
		disableTable(tableName);
		hAmin.deleteTable(tableName);
		hAmin.close();
	}

	/**
	 * 判定表是否存在
	 *
	 * @param tableName 表名
	 */
	private static boolean existsTable(String tableName) throws IOException {
		HBaseAdmin hAmin = (HBaseAdmin) conn.getAdmin();
		boolean result = hAmin.tableExists(tableName.getBytes());
		hAmin.close();
		return result;
	}

	/**
	 * 插入hbase中获得数据，傳入表名tableName,行键rowkey,列族cf,列名column,值value.
	 *
	 * @param tableName 表名
	 * @param rowKey    行键
	 * @param cf        列族
	 * @param column    列
	 * @return 返回要查询的值
	 * @throws IOException 声明异常
	 */
	public static String getValue(String tableName, String rowKey,
								  String cf, String column) throws IOException {
		Table table = conn.getTable(TableName.valueOf(tableName));
		Get get = new Get(rowKey.getBytes());
		get.addColumn(cf.getBytes(), column.getBytes());
		String val = null;
		Result result = table.get(get);
		if (result.value() != null) {
			val = new String(result.value());
		}
		table.close();
		return val;
	}

	/**
	 * 获取行键下的所有值
	 */
	public static Result getResult(String tableName, String rowkey,
								   String cf) throws IOException {
		Table htable = conn.getTable(TableName.valueOf(tableName));
		Get get = new Get(rowkey.getBytes());
		get.addFamily(cf.getBytes());
		htable.close();
		return htable.get(get);
	}

	/**
	 * 向某一行键的一列族传入多个列值
	 */
	public static void putToHBase(String tableName, String rowkey,
								  String cf, String[] column, String[] value) throws Exception {
		if (null == column || null == value) {
			throw new Exception("column OR value invalid");
		}
		if (column.length != value.length) {
			throw new Exception("column.lenth must equals value.lenth");
		}

		Table htable = conn.getTable(TableName.valueOf(tableName));
		Put put = new Put(rowkey.getBytes());
		for (int i = 0; i < column.length; i++) {
			put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column[i]), Bytes.toBytes(value[i]));
		}
		htable.put(put);
		htable.close();
	}

	/**
	 * 向表中批量插入数据
	 */
	public static void putToHBase(String tableName, List<Put> puts) throws IOException {
		if (puts.isEmpty()) {
			return;
		}
		Table htable = conn.getTable(TableName.valueOf(tableName));
		htable.put(puts);
		htable.close();
	}

	/**
	 * 删除hbase中rokey下某个列族的所有值
	 */
	public static void deleteRowkeyByCF(String tableName, String rowkey, String cf) throws IOException {
		Table htable = conn.getTable(TableName.valueOf(tableName));
		Delete delete = new Delete(rowkey.getBytes());
		delete.addFamily(cf.getBytes());
		htable.delete(delete);
		htable.close();
	}

}
