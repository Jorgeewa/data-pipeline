# data-pipeline
![data-pipeline](personal_project_data_engineering.drawio.png?raw=true "Title")


This project implements a data pipeline as seen in the image. 

- A robot simulates sending sensor data (temperature and humidity) with an ecs instance
- The data is ingested with Kinesis firehose held up for 5 minutes and put into s3
- An s3 notification is triggered into sqs
- Another ecs instance polls the sqs for data, transforms it, saves it to a different folder and triggers events and glue jobs.
- A glue job performs the ETL ie extracting the data from s3 and into a redshift data base.
- Another ecs instance extracts the data from the redshift data base, makes graphs and maps with it and saves the output in a dynamodb.


Additionally the project will try to:
- Generate insights from the data in redshift with amazon quicksight
- Consolidate the CI/CD with jenkins
- Create Ias using terraform
