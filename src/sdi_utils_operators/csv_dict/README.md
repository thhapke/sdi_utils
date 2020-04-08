# csv stream to dict - sdi_utils_operators.csv_dict (Version: 0.0.1)

Converts csv stream to dict

## Inport

* **stream** (Type: message) Input json byte or string

## outports

* **log** (Type: string) Logging data
* **data** (Type: message.Dictionary) Output data as dictionary

## Config

* **debug_mode** - Debug mode (Type: boolean) Sending debug level information to log port
* **collect** - Collect data (Type: boolean) Collect data before sending it to the output port
* **separator** - Separator (Type: string) Separator


# Tags
pandas : sdi_utils : 

