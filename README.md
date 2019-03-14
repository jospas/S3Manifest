# S3Manifest

Sample code for building a CSV manifest of huge S3 buckets for analytics.

It generates count and total size statistics per storage type and produces a gzipped, comma separated CSV file with the following structure:

	"Path","Name","StorageClass","Size"
	"css/","file1.css","STANDARD",144832
	"js/","app.js","STANDARD",5222
	"","favicon.ico","STANDARD",1150

These files are directly consumable by Amazon Athena for analytics.

### Building


The system uses Apache Maven and is easily configured in IntelliJ by importing the Maven pom.xml as a new project.

Build either from within IntelliJ or use Maven from the command line:

```bash
mvn package
```

The system builds an executable Jar file containing all required jars:

	target/S3Manifest-2.0.jar

### Usage

To run the program from the command line use:

	java -jar target/S3Manifest-2.0.jar
	
This produces help documentation:

	usage: java -jar target/S3Manifest-2.0.jar
	    --bucket <arg>    Required S3 bucket name to enumerate
	    --output <arg>    Required output CSV file (.gz is appended)
	    --prefix <arg>    Optional S3 prefix to list under
	    --profile <arg>   Optional AWS profile name to use
	    --region <arg>    Required AWS region for the bucket

### Authors

**Josh Passenger** AWS Solutions Architect - [jospas@amazon.com](mailto:jospas@amazon.com)

### License

This project is licensed under the Apache 2 License - see the [LICENSE](LICENSE) file for details

### Warranty

No warranty is provided or implied with this software,
it is provided as a Proof of Concept (POC) example and will require additional error checking code and testing.

