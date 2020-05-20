# csv stream to table - sdi_utils_operators.csv_table (Version: 0.0.1)

Converts csv stream to table

## Inport

* **stream** (Type: message) Input csv byte or string

## outports

* **log** (Type: string) Logging data
* **data** (Type: message.dicts) Output data as list of dictionaries

## Config

* **debug_mode** - Debug mode (Type: boolean) Sending debug level information to log port
* **collect** - Collect data (Type: boolean) Collect data before sending it to the output port
* **has_header** - Has_header (Type: boolean) If csv-file has a header.
* **separator** - Separator (Type: string) Separator


# Tags
sdi_utils : 

