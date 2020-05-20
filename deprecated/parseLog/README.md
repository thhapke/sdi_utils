# Parse Logs - sdi_utils_operators.parseLog (Version: 0.0.1)

Parse logs for keywords to control process flow, e.g. stop a branched pipeline consistently

## Inport

* **log** (Type: string) Input log statements

## outports

* **log** (Type: string) Output of unchanged log statements
* **index** (Type: integer) Control index

## Config

* **debug_mode** - Debug mode (Type: boolean) Sending debug level information to log port
* **marker_map** - List of marker (Type: string) List of keywords of operators that send an integer to output
* **sleep** - Sleep (Type: integer) Time before starting next processing step
* **separator** - Separator (Type: string) Separates fields of logging stream


# Tags
pandas : sdi_utils : 

