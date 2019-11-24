# textfield_parser
Getting a string from a textfield and returns the parsed content. In general there are 3 types of saparators: 

* 'sep': the usual one, default: ','
* 'outer_sep': if there are 2 lists this defines the list separator containing a list, default: ';'
* 'map_sep': for separating a value pair like e.g. dictionaries, default: ':'

The expected syntax is quite easy and appropriate to the limited space of normal textfields. You can use the brackets '()' or '{}' but they are ignored. 

All parsing functions enable the detection of integers and floats if the flag *test_number* is set True (default). 

**WARNING**: 
Quotes help you to protect values with spaces but not the characters used for separating elements! This is in particular important to be aware of when using *float* values with 'decimals' or 'thousands'.

## read_value
Reads a string and strips spaces and quotes. 

`def read_value(text,test_number = True)`

### Arguments 
* **text** -string- from textfield that will be parsed
* **test_number** -boolean- if True (default) integers or float values are interpreted as such. 

### Return
* stripped string


## read_list
Reads a comma separated list and returns either the items or the remainder of the additional list when a text contains the modifier 'Not'. If only ['All','all','ALL'] is found then the value_list is returned.  

`def read_list(text,value_list=None,sep = ',',modifier_list_not=None,test_number = True)`

### Arguments 
* **text** -string- from textfield that will be parsed
* **value_list** -list- list from which the parsed text elements will be removed when there is a modifier *'NOT'*. 
* **sep** -char- element separator in string
* **modifier_list** -list- List of modifier for excluding the listed items from value_list
Default: ['!', '~', 'not', 'Not', 'NOT']
* **test_number** -boolean- if True (default) integers or float values are interpreted as such. 


### Return
* List of parsed parameters


### Examples

**Simple list: **

* text = "'Hello', 'a list', separated by , me "  

-> ['Hello', 'a list', 'separated by', 'me']

**Not list**

* text = "Not Mercedes, Renault, Citroen, Peugeaut, 'Rolls Royce'" 
* value_list = ['Mercedes', 'Audi', 'VW', 'Skoda', 'Renault', 'Citroen', 'Peugeot', 'Rolls Royce']

to

`['Audi', 'VW', 'Skoda', 'Peugeot'] `
 
## read\_dict\_of_lists
Reads a 'outer_sep'-separated list of a mapping with 'inner-sep' separated list and returns a dictionary with the 
map as key and a list as value. 

`read_dict_of_list(text,inner_sep = ',',outer_sep = ';',map_sep=':',test_number = True)`

### Arguments 

* **text** -string- from textfield that will be parsed
* **inner_sep** -char- element separator of value list 
* **outer_sep** -char- list separator for the lists
* **map_sep** -char- separates map from list
* **test_number** -boolean- if True (default) integers or float values are interpreted as such. 

### Return
* Dictionary of the keys and lists.

### Example
"'Mercedes':expensive, German, respectable; Audi:'sportive, German, technology-advanced'; VW : 'people', 'solid', No1; Citroen:cool, Fantomas, CV2, elastic ; 'Rolls Rocye': royal,British, 'very expensive', black"

to

`{'Mercedes': ['expensive', 'German', 'respectable'], 'Audi': ['sportive', 'German', 'technology-advanced'], 
'VW': ['people', 'solid', 'No1'], 'Citroen': ['cool', 'Fantomas', 'CV2', 'elastic'], 'Rolls Rocye': ['royal', 'British', 'very expensive', 'black']}
Map :{'Mercedes': 'expensive', 'Audi': 'sportive', 'VW': 'people', 'Citroen': 'cool', 'Rolls Rocye': 'royal'}`


## read_dict
Reads a comma separated list of mappings and returns a dictionary. 

`def read_dict(text, sep =',', map_sep=':', test_number = True)`

### Arguments
* **text** -string- from textfield that will be parsed
* **sep** -char- element separator in string
* **test_number** -boolean- if True (default) integers or float values are interpreted as such. 

### Return
* dictionary of parsed parameters

### Example
"'Mercedes':expensive, Audi:'sportiv', VW : 'people', Citroen:cool, 'Rolls Rocye': royal"

to 

`{'Mercedes': 'expensive', 'Audi': 'sportiv', 'VW': 'people', 'Citroen': 'cool', 'Rolls Rocye': 'royal'}`

## read\_dict\_of\_dict
Reads a dictionary of dictionaries and returns a dictory with a 2-level hierarchy.

`def read_dict_of_dict(text, sep=',', map_sep=':', outer_sep = ';', test_number = True)`

### Arguments
* **text** -string- from textfield that will be parsed
* **sep** -char- element separator in string
* **map_sep** -char- element separator for pairs of mappings
* **outer_sep** -char- list separator for the list containing a list
* **test_number** -boolean- if True (default) integers or float values are interpreted as such. 

### Return
* dictionary of parsed parameters

### Example
"'High Class':{'Mercedes':expensive,'BWM':'sporty};'Sport Class': Porsche:'None',Ferrari:special; 'Middle Class':{VW : 'people','Renault':'fashionable',Citroen:'classy, 'Peugeot':'modern'};'Luxury Class':{'Rolls Rocye': royal, Bentley:'rare'}; 'All:{4:8.9,6:90,7:7}"

to: 

`{'High Class': {'Mercedes': 'expensive', 'BWM': 'sporty'}, 'Sport Class': {'Porsche': None, 'Ferrari': 'special'}, 'Middle Class': {'VW': 'people', 'Renault': 'fashionable', 'Citroen': 'classy', 'Peugeot': 'modern'}, 'Luxury Class': {'Rolls Rocye': 'royal', 'Bentley': 'rare'}, 'All': {4: 8.9, 6: 90, 7: 7}}`



## read_json
Reads a json formatted string and return a dictionary 

### Arguments
* **text** -string- json-string

### Return
* dictionary of json

### Example
"{\"Luxury Class\": {\"Mercedes\":\"expensive\",\"Rolls Rocye\": \"royal\"}, \"High Middle Class\":{\"Audi\":\"sportiv\"}, \"Middle Class\" : {\"Citroen\":\"cool\",\"VW\" : \"people\" }}"

to

`{'Luxury Class': {'Mercedes': 'expensive', 'Rolls Rocye': 'royal'}, 'High Middle Class': {'Audi': 'sportiv'}, 'Middle Class': {'Citroen': 'cool', 'VW': 'people'}}`

## read_comparisons
Parses a list of comparisons and returns a list of lists with 3 items: (left, comparison-operator, right). There is an internal mapping of comparison characters: {'!=':'!','==':'=','>=':'≥','=>':'≥','<=':'≤','=<':'≤'} 

`def read_comparisons(text,sep = ',',formula_map = None)`

### Arguments
* **text** -string- Textfield string
* **sep** -char- element separator in string
* **modifier_map** -dictionary- mapping of comparison strings to 1-char comparison. 
Default: {'!=': '!', '==': '=', '>=': '≥', '=>': '≥', '<=': '≤', '=<': '≤'}
`

### Return
* List of 3 element lists

### Examples

' anna > 1.70, norbert != 900, cindy <= 1.65'

to 

`[['anna', '>', 1.7], ['norbert', '!', 900.0], ['cindy', '≤', 1.65]]`



