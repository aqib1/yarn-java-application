package com.yarn.consts;

import org.apache.hadoop.yarn.api.ApplicationConstants;

public class Constants {

	public static final String HELP_KEY = "help";

	public static final String HELP_KEY_DESC = "Help print message";

	public static final String PRIORITY_KEY = "priority";

	public static final String PRIORITY_KEY_DESC = "Application priority, with default value 0";

	public static final String NUM_CONTAINER_KEY = "num_containers";

	public static final String NUM_CONTAINER_KEY_DESC = "Number of containers, where commands going to be executed";

	public static final String CONTAINER_VCORES_KEY = "container_vcores";

	public static final String CONTAINER_VCORES_DESC = "Virtual cores, for running shell command";

	public static final String APP_ATTEMPT_ID_KEY = "app_attempt_id";

	public static final String APP_ATTEMPT_ID_DESC = "application attempt id, used for testing purpose";

	public static final String SHELL_ENV_KEY = "shell_env";

	public static final String SHELL_ENV_KEY_DESC = "Environment for shell script. Specified as env_key=env_val pairs";

	public static final String CONTAINER_MEMORY_KEY = "container_memory";

	public static final String CONTAINER_MEMORY_KEY_DESC = "Amount of memory for container in MB, requested using shell";

	public static final String JAVA_BIN_PATH = "$JAVA_HOME/bin/java";
	
	public static final String SPARK_SUBMIT_COMMAND = "spark-submit --class ";

	public static final String JAVA_MAX_MEMORY = " -Xmx256M";

	public static final String YARN_APP_MEMORY_STATS = " com.yarn.details.YarnDetails";
	
	public static final String SPAKR_JOB_APP = "com.spark.read.SparkJob";

	public static final String LOG_DIR_STDOUT = " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout";

	public static final String LOG_DIR_STDERR = " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";

	public static final String AM_JAR_PATH = "AM_JAR_PATH";
	
	public static final String SPARK_JAR = "SPARK_JAR_PATH";

	/**
	 * Environment key name denoting the file timestamp for the shell script. Used
	 * to validate the local resource.
	 */
	public static final String AM_JAR_TIMESTAMP = "AM_JAR_TIMESTAMP";

	/**
	 * Environment key name denoting the file content length for the shell script.
	 * Used to validate the local resource.
	 */
	public static final String AM_JAR_LENGTH = "AM_JAR_LENGTH";

	public static final String AM_JAR_NAME = "AppMaster.jar";
	
	public static final String SPARK_JAR_NAME = "SparkJob.jar";
}
