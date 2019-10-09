package dev.nifi.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * CLI to run commands against NiFi.
 *
 */
public class Main {

	public static void main(String[] args) {
		Options options = new Options();
        
		// Commands
        Option importYaml = new Option("i", "Import all YAML files from specified directory (Default: '.') into NiFi workspace.");
        importYaml.setLongOpt("import");
        importYaml.setArgs(1);
        importYaml.setArgName("directory");
        importYaml.setOptionalArg(true);
        
        Option export = new Option("x", "Export NiFi workspace to yaml and save all yaml files to directory (Default: '.').");
        export.setLongOpt("export");
        export.setArgs(1);
        export.setArgName("directory");
        export.setOptionalArg(true);
        
        Option clean = new Option("clean", "Clean the workspace of all content.");
        
        Option start = new Option("start", "Start all processors in workspace.");
        
        Option stop = new Option("stop", "Stop all processors in workspace.");
        
        Option help = new Option("h", "help", false, "Display help information.");
        
        // Options
        Option verbose = new Option("v", "verbose", false, "Enables verbose output.");
        
        // Commands
        options.addOption(export);
        options.addOption(importYaml);
        options.addOption(clean);
        options.addOption(start);
        options.addOption(stop);
        options.addOption(help);
        
        // Additional options
        options.addOption(verbose);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
            
            if (cmd.hasOption("h") || args.length == 0) {
            	formatter.printHelp("templatizer", options);
            }
        } catch (ParseException e) {
            System.out.println(e.getMessage());
        	formatter.printHelp("templatizer", options);
        }
	}
}
