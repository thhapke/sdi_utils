# SAP Data Intelligence Utilites (sdi_utils) by thhapke

## sdi_utils Package

This package includes a couple of helpers that ease the development of operators for use with 
 **SAP Data Intelligence (SAP DI)**.  
 
 * [**gensolution**](./gensolution.md) for generating solution packages from locally developed operators. 
 *  [**textfield_parser**](./textfield_parser.md) that parses textfields that could contain lists, maps, list of maps,  etc. 
 *  [**set_logging**](./set_logging.md) for channeling logging output to a string for tap wiring it with separate monitor
 *  [**tprogress**](./tprogress.md) for a simple time keeping to check performance of certain operators (tasks)


### Installation
 ```
 pip install sdi_utils
 ```
 
 
## SAP Data Intelligence Helper Operators
 
I created a some operators which have been reused a couple of times. Maybe they provide a quick solution for your pipelines as well. Please be aware that many of them become obsolete with the next releases in particular when the looming *vtypes* are introduced. 

You find the source code of all operators in [**src/sdi_utils_operators/**](.src/sdi_utils_operators/). They have been created outside a SAP Data Intelligence instance and with **gensolution** the solution has been created that can then directly been uploaded. But all generated solutions you also find in the folder [**sdi_utils/solution/operators/**](./sdi_utils/solution/operators/). 

All operators require the modules packaged in **sdi_utils**, so you have to add at least this to your docker image. For additional packages have a look at the tags added to each operator. 

The following list might not encompass all operators you find in the source or solutions folder. Because I am not been able to maintain this document in sync with the operators I create constantly.

 Operator | Description
 --- | ---
 csv_df | Converts a csv byte/string stream into a pandas DataFrame
 csv_dict | Converts a csv byte/string stream into a list of dictionaries with header as key
 csv_table | Converts a csv byte/string stream into a 2 dimensional array. Used for the beta Hana Operator only. 
 df_csv | Converts a DataFrame into a csv-string for saving with WriteFile-operator
 df_table | Converts a DataFrame into a 2 dimensional array with the according attributes used for writing in a HANA Database (Beta-operator
 dict_df | Converts a dictionary into DataFrame
 dict_json | Converts a dictionary into a JSON-string
 dict_table | Converts a dictionary into a 2-dimensional array
 filter_date | When the date is part of the filename (format: yyyy-mm-dd) then files can be selected with dates within the period stated in the configuration
 count_gate | Decision gate that triggers next processing step only after the number stated in the configuration has been reached. The number can also be set dynamically either directly via the limit port or via attribute port. This operator is mostly used to terminate a pipeline.
 HTTPdownload | Downloads the file given in the url
 json_df | Converts a json-string into a DataFrame
 json_df | Converts a json-string into a dictionary
 line_array | Converts a byte-stream into an 1-dimensional array
 table_csv | Converts a 2-dimensional array into a csv. Using the attributes of the *beta*-type table for the header. 
 
## Operator Testing and Modifying
If you like to test and modify the operators outside of Data Intelligence and create the operator-solution automatically you have to install vctl and sdi_utils. Then you can add the following code-snippet at the end of the operator-code: 

```
if __name__ == '__main__':
    test_operator()
    subprocess.run(["rm", '-r','<projectfolder>/solution/operators/sdi_utils_operators' + api.config.version])
    gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
    solution_name = api.config.operator_name + '_' + api.config.version
    subprocess.run(["vctl", "solution", "bundle", '<projectfolder>/solution/operators/sdi_utils_operators_' + api.config.version, "-t", solution_name])
    subprocess.run(["mv", solution_name + '.zip', '../../../solution operators'])
```

 
 
## Github
 [github repository](https://github.com/thhapke/sdi_utils)