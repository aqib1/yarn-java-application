package com.yarn.application;

import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.util.Records;

public class YarnApplicationMaster {

	private ContainerLaunchContext createContainerLaunchContext(LocalResource appMasterJar, Map<String, String> containerEnv) {
		ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);
		context.setLocalResources(
				Collections.singletonMap(Constants.AM_, value)
				
				
				);
		
	}
}
