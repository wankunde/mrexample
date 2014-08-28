package com.wankun.apache_common.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/*   
 输入参数  -c code -t time   
 运行结果:   
 code   
 null   
 */
public class CliTest {
	/**
	 * @param args
	 * @throws ParseException
	 */
	public static void main(String[] args) throws ParseException {

		Options options = new Options();
		options.addOption("t", false, "display current time"); // 参数不可用，程序不会处理该参数
		options.addOption("c", true, "country code"); // 参数可用

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);

		if (cmd.hasOption("c")) {
			String countryCode = cmd.getOptionValue("c");
			System.out.println(countryCode);
		}

		if (cmd.hasOption("t")) {
			String time = cmd.getOptionValue("t");
			System.out.println(time);
		}
		
		HelpFormatter formatter = new HelpFormatter();  
		formatter.printHelp( "ant", options ); 
	}
}