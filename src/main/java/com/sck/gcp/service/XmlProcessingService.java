package com.sck.gcp.service;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.multipart.MultipartFile;

import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.sck.gcp.processor.FileProcessor;

@Service
public class XmlProcessingService {
	private static final Logger LOGGER = LoggerFactory.getLogger(XmlProcessingService.class);

	@Autowired
	private BigQueryService bigQueryService;

	@Autowired
	private FileProcessor fileProcessor;

	public static int PRETTY_PRINT_INDENT_FACTOR = 4;

	public ListenableFuture<Job> convertAndUpload(String tableName, MultipartFile inputFile) {
		String xml;
		if (null != inputFile) {
			xml = fileProcessor.readFile(inputFile);
		} else {
			xml = fileProcessor.readFile();
		}
		LOGGER.info("readFile() completed");
		String json = fileProcessor.convertToJSONL(xml);
		LOGGER.info("convertToJSON() completed");
		InputStream dataStream = new ByteArrayInputStream(json.getBytes());
		LOGGER.info("ByteArrayInputStream() completed");
		ListenableFuture<Job> payloadJob = bigQueryService.writeFileToTable(tableName, dataStream,
				FormatOptions.json());
		LOGGER.info("Upload completed to Table:" + tableName);
		return payloadJob;
	}

}
