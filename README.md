**BPCM Agent**

The BPCM Agent is a Python script designed to monitor specified directories for new files, validate them based on predefined criteria, and process them by moving them to a designated input directory and logging their metadata into a MySQL database. This agent is particularly useful for managing and automating file processing workflows, such as playlist management in media systems.

Features
Directory Monitoring: Watches multiple directories for new files.

File Validation: Validates files based on prefix, date format, and file extension.

Database Integration: Logs file metadata and processing status into a MySQL database.

Error Handling: Logs errors and validation failures for troubleshooting.

Polling Mechanism: Continuously scans directories at regular intervals.

Requirements
Python 3.x

MySQL Connector/Python (mysql-connector-python)

Configuration file (/etc/mdmgr/<customer_name>.ini)
