# Parse Logs - sdi_utils_operators.gate (Version: 0.0.1)

Parse logs for keywords to control process flow, e.g. stop a branched pipeline consistently

## Inport

* **limit** (Type: integer) Limit
* **data** (Type: message) Input data

## outports

* **data** (Type: message) Output of unchanged last input data

## Config

* **debug_mode** - Debug mode (Type: boolean) Sending debug level information to log port
* **limit** - Limit (Type: integer) Limit after which the message is send
* **sleep** - Sleep (Type: integer) Time before starting next processing step


# Tags
pandas : sdi_utils : 

