# Pivotize table - sdi_utils_operators.table.pivotize (Version: 0.0.1)

Sends input topics to SQL processor and topic frequency operator.

## Inport

* **topic** (Type: message.table) Message with body as table.

## outports

* **log** (Type: string) Logging data
* **table** (Type: message) Message with pivotized table

## Config

* **debug_mode** - Debug mode (Type: boolean) Sending debug level information to log port
* **ref_col_index** - Reference column index (Type: integer) Reference column index
* **pivot_cols_index** - Pivot columns index list (Type: string) Pivot columns index list


# Tags
sdi_utils : 

