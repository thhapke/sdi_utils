# Gate - sdi_utils_operators.gate (Version: 0.1.0)

Gate that counts incoming messages and sends out message with latest message when gate limit has been reached.

## Inport

* **limit** (Type: int64) Limit
* **attributes** (Type: message) Message with attributes to used
* **data** (Type: message.*) Input data

## outports

* **log** (Type: string) Logging data
* **trigger** (Type: string) Trigger next process step
* **data** (Type: message) Output of unchanged last input data

## Config

* **debug_mode** - Debug mode (Type: boolean) Sending debug level information to log port
* **limit** - Limit (Type: integer) Limit after which the message is send
* **sleep** - Sleep (Type: integer) Time before starting next processing step
* **attribute** - Attribute (Type: string) Attribute that keeps a number used as limit, e.g. storage.fileCount


# Tags
pandas : sdi_utils : 

