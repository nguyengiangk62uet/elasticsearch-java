package com.thesis;

import controller.DocumentController;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import service.DocumentService;

import java.io.File;

@SpringBootApplication
@ComponentScan(basePackageClasses = DocumentController.class)
public class ElasticsearchJavaApplication {

	public static void main(String[] args) {
		SpringApplication.run(ElasticsearchJavaApplication.class, args);

		DocumentController documentController = new DocumentController();
		File folder = new File("D:\\Big data source\\biggest");
		documentController.indexIntoElasticsearch(folder);
	}

}
