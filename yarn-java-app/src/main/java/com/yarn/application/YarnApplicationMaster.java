package com.yarn.application;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.yarn.consts.Constants;

public class YarnApplicationMaster {
	
	private static final Log LOG = LogFactory.getLog(YarnApplicationMaster.class);

	// Application Attempt id
	private ApplicationAttemptId appAttemptId;

	// Yarn configurations
	private Configuration conf;

	private String appJarPath = "";

	// TimeStamp needed for creating a local resource
	private long appJarTimeStamp = 0;
	// File length needed for local resource
	private long appJarPathLen = 0;

	public boolean init(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(Constants.APP_ATTEMPT_ID_KEY, true, Constants.APP_ATTEMPT_ID_DESC);
		options.addOption(Constants.SHELL_ENV_KEY, true, Constants.SHELL_ENV_KEY_DESC);
		options.addOption(Constants.CONTAINER_MEMORY_KEY, true, Constants.CONTAINER_MEMORY_KEY_DESC);
		options.addOption(Constants.CONTAINER_VCORES_KEY, true, Constants.CONTAINER_MEMORY_KEY_DESC);
		options.addOption(Constants.NUM_CONTAINER_KEY, true, Constants.NUM_CONTAINER_KEY_DESC);
		options.addOption(Constants.PRIORITY_KEY, true, Constants.PRIORITY_KEY_DESC);
		options.addOption(Constants.HELP_KEY, false, Constants.HELP_KEY_DESC);

		CommandLine commandLine = new GnuParser().parse(options, args);
		checkEnviromentVariables(commandLine);
		return false;
	}

	private void checkEnviromentVariables(CommandLine commandLine) {
		Map<String, String> env = System.getenv();
		if (!env.containsKey(ApplicationConstants.Environment.CONTAINER_ID.name())) {
			if (commandLine.hasOption("app_attempt_id")) {
				String appIdStr = commandLine.getOptionValue("app_attempt_id", "");
				appAttemptId = ConverterUtils.toApplicationAttemptId(appIdStr);
			} else {
				throw new IllegalArgumentException("Application id is not set in enviroment");
			}
		} else {
			ContainerId containerId = ConverterUtils
					.toContainerId(env.get(ApplicationConstants.Environment.CONTAINER_ID.name()));
			appAttemptId = containerId.getApplicationAttemptId();
		}

		if (!env.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
			throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV + " not set in the enviroment");
		}
		if (!env.containsKey(ApplicationConstants.Environment.NM_HOST.name())) {
			throw new RuntimeException(ApplicationConstants.Environment.NM_HOST.name() + " not set in the enviroment");
		}
		if (!env.containsKey(ApplicationConstants.Environment.NM_HTTP_PORT.name())) {
			throw new RuntimeException(
					ApplicationConstants.Environment.NM_HTTP_PORT.name() + " not set in the enviroment");
		}
		if (!env.containsKey(ApplicationConstants.Environment.NM_PORT.name())) {
			throw new RuntimeException(ApplicationConstants.Environment.NM_PORT.name() + " not set in the enviroment");
		}
		if (env.containsKey(Constants.AM_JAR_PATH)) {
			appJarPath = env.get(Constants.AM_JAR_PATH);
		}
		if (env.containsKey(Constants.AM_JAR_TIMESTAMP)) {
			appJarTimeStamp = Long.valueOf(env.get(Constants.AM_JAR_TIMESTAMP));
		}
		if (env.containsKey(Constants.AM_JAR_LENGTH)) {
			appJarPathLen = Long.valueOf(env.get(Constants.AM_JAR_LENGTH));
		}

		if (!appJarPath.isEmpty() && (appJarTimeStamp <= 0 || appJarPathLen <= 0)) {
			LOG.error("Illegal values in enviroments for shell script path" + ", path=" + appJarPath + ", len=" + appJarPathLen
					+ ", timestamp=" + appJarTimeStamp);
			throw new IllegalArgumentException("Illegal values in enviroment for shell script path");
		}
		
	    LOG.info("Application master for app" + ", appId="
	            + appAttemptId.getApplicationId().getId() + ", clusterTimestamp="
	            + appAttemptId.getApplicationId().getClusterTimestamp()
	            + ", attemptId=" + appAttemptId.getAttemptId());
	}

	public YarnApplicationMaster() {
		conf = new YarnConfiguration();
	}

	private LocalResource createAppMasterJar() throws IOException {
		LocalResource appMasterJar = Records.newRecord(LocalResource.class);
		appMasterJar.setType(LocalResourceType.FILE);
		if (!appJarPath.isEmpty()) {
			Path jarPath = new Path(appJarPath);
			jarPath = FileSystem.get(conf).makeQualified(jarPath);
			appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
			appMasterJar.setTimestamp(appJarTimeStamp);
			appMasterJar.setSize(appJarPathLen);
			appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
		}
		return appMasterJar;
	}

	private ContainerLaunchContext createContainerLaunchContext(LocalResource appMasterJar,
			Map<String, String> containerEnv) {
		ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);
		context.setLocalResources(Collections.singletonMap(Constants.AM_JAR_NAME, appMasterJar));
		context.setEnvironment(containerEnv);
		context.setCommands(Collections.singletonList(Constants.JAVA_BIN_PATH + Constants.YARN_APP_MEMORY_STATS
				+ Constants.LOG_DIR_STDOUT + Constants.LOG_DIR_STDERR));
		return context;
	}
}
