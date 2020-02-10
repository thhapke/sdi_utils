# scrapy executor - sdi_utils_operators.scrapy_executor (Version: 0.0.1)

Starts scrapy and sends output to data port and stderr to log port.

## Inport

* **trigger** (Type: string) Trigger

## outports

* **log** (Type: string) Logging
* **stderr** (Type: string) Stderr
* **data** (Type: message) data

## Config

* **debug_mode** - Debug mode (Type: boolean) Sending debug level information to log port
* **cmd** - Command (Type: string) Command to be executed
* **scrapy_dir** - Scrapy Directory (Type: string) Scrapy directory on container


# Tags
pandas : sdi_utils : 

