package com.sck.gcp.service;

import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.spring.bigquery.core.BigQueryTemplate;

@Service
public class BigQueryService {
	private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryService.class);

	/*
	 * @Autowired BigQueryFileGateway bigQueryFileGateway;
	 */

	@Autowired
	BigQueryTemplate bigQueryTemplate;

	public ListenableFuture<Job> writeFileToTable(String tableName, InputStream dataStream, FormatOptions fileFormat) {
		LOGGER.info("Table Name: " + tableName);
		LOGGER.info("Dataset Name: " + this.bigQueryTemplate.getDatasetName());
		LOGGER.info("FileFormat: " + fileFormat);
		return this.bigQueryTemplate.writeDataToTable(tableName, dataStream, fileFormat);
	}
}
