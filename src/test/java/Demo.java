import org.junit.Test;


public class Demo {
	@Test
	public void test() {
		test2();

	}

	private void test2(String... args){
		if (args.length==0){
			System.out.println("参数异常");
		}
	}
}
