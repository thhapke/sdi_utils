# scrapy executor - sdi_utils_operators.scrapy2 (Version: 0.0.1)

Starts scrapy and sends output to data port and stderr to log port.

## Inport

* **spider** (Type: message) spider.py file
* **pipelines** (Type: message) pipelines.py file
* **items** (Type: message) pipelines.py file
* **middlewares** (Type: message) middlewares.py file
* **settings** (Type: message) settings.py file

## outports

* **log** (Type: string) Logging
* **stderr** (Type: string) Stderr
* **data** (Type: message) data

## Config

* **debug_mode** - Debug mode (Type: boolean) Sending debug level information to log port
* **cmd** - Command (Type: string) Command to be executed
* **scrapy_dir** - Scrapy Directory (Type: string) Scrapy directory on container
* **project_dir** - Project Directory (Type: string) Scrapy project directory on container
* **start_cmd** - Start command (Type: boolean) Start command


# Tags
pandas : sdi_utils : scrapy : 

