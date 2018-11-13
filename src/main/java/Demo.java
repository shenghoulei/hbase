import util.HBaseOperateUtil;

public class Demo {
	public static void main(String[] args) {
		try {
			String result = HBaseOperateUtil.getValue("ssjt:test", "row", "info", "name");
			System.out.println(result);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
