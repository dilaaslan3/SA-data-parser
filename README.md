# SA-data-parser
This Python script parses the data that streamed through Kafka, used in Software Architecture course (University of L'Aquila, 2020) final project.

# Features
  - Consume the raw data from Kafka topic
  - Decode the payload of consumed data from Base64 string to JSON string
  - Convert the JSON string to JSON data (Python dictionary)
  - Convert the JSON data to CSV, write it to CSV file
  - Upload the CSV data file to Azure Blob Storage to be used by model training process in Azure Machine Learning

# Dependencies & Installation


```sh
$ pip3 install kafka-python
$ pip3 install azure-storage-blob
```
 # Usage

To make script work, `AZURE_STORAGE_CONNECTION_STRING` environment variable has to be set.
(It's needed to upload the CSV file to Azure Blob Storage) 

Also, this script written to be work with Kafka Producer in https://github.com/cristinaStratan1/SA-project

To run it:
```sh
$ python3 main.py
```
# Contributors
 - Dila Aslan github.com/dilaaslan3
 - Mustafa Yumurtacı - github.com/mstfymrtc
# License


Distributed under the MIT. See LICENSE for more information.

