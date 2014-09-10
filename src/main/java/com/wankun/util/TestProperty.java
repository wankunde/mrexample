package com.wankun.util;

/**
 * 
 * <pre>
 * 测试java环境变量 java.library.path
 * 1. 默认值为： /usr/java/packages/lib/amd64:/usr/lib64:/lib64:/lib:/usr/lib
 * 2. 命令行指定会覆盖变量值。例如： java -Djava.library.path=/jni/library/path
 * 3. 修改LD_LIBRARY_PATH会同步更改java.library.path变量值
 * </pre>
 * 
 * @author wankun
 * @date 2014年9月3日
 * @version 1.0
 */
public class TestProperty {

	public static void main(String[] args) {
		System.out.println(System.getProperty("java.library.path"));
	}
}
