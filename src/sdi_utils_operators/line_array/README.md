# Line to array - sdi_utils_operators.line_array (Version: 0.0.1)

Converts lines to array

## Inport

* **stream** (Type: message) Input csv byte or string

## outports

* **log** (Type: string) Logging data
* **data** (Type: message.list) Output data as list

## Config

* **debug_mode** - Debug mode (Type: boolean) Sending debug level information to log port
* **collect** - Collect data (Type: boolean) Collect data before sending it to the output port
* **lexicographical** - Lexicographical order (Type: boolean) Order array according to lexicographical order 


# Tags
sdi_utils : 

