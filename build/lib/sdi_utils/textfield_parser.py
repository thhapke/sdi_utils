import json
import re
import logging

tflogger = logging.getLogger('textfield_parser')


#### READ Value
def read_value(text,test_number = True):
    if not text or text.upper() == 'NONE':
        return None
    val = text.strip().strip("'").strip('"')
    if test_number :
        val = number_test(val)
    tflogger.debug('Text -> Result: {} -> {} '.format(text,val))
    return  val

#### READ LIST
def read_list(text,value_list=None,sep = ',',modifier_list_not=None,test_number = True):

    if not text or text.upper() == 'NONE':
        return None

    if not modifier_list_not:
        modifier_list_not = ['!', '~', 'not', 'Not', 'NOT']

    text = text.strip()

    # test for all
    if len(text) < 4 and ('all' in text or 'All' in text or 'ALL' in text) :
        return value_list

    negation = False
    # Test for Not
    if len(text) > 1 and text[0] in modifier_list_not :
        text = text[1:]
        negation = True
    elif len(text) > 3 and text[:3] in modifier_list_not :
        text = text[4:].strip()
        negation = True

    result_list = list()
    elem_list = text.split(sep)
    for x in  elem_list:
        elem = x.strip().strip("'\"")
        if test_number :
            elem = number_test(elem)
        result_list.append(elem)

    if negation :
        if not isinstance(value_list,list) :
            value_list = list(value_list)
        if not value_list  :
            raise ValueError("Negation needs a value list to exclude items")
        result_list = [x for x in value_list if x not in result_list]

    tflogger.debug('Text -> Result: {} -> {} '.format(text, result_list))
    return result_list

#### READ VALUE LIST
def read_dict_of_list(text,inner_sep = ',',outer_sep = ';',map_sep=':',test_number = True):
    if not text or text.upper() == 'NONE':
        return None
    vo_lists = [x.strip().strip("'\"") for x in text.strip().split(outer_sep)]

    value_list_dict = dict()
    for vo in vo_lists :
        key, vi_str = vo.split(map_sep)
        key = key.strip().strip("'\"")
        vi_list = vi_str.split(inner_sep)
        mapped_list = list()
        for vi in vi_list :
            elem = vi.strip().strip("'\"")
            if test_number :
                elem = number_test(elem)
            mapped_list.append(elem)
        value_list_dict[key] = mapped_list

    return value_list_dict


#### READ DICT
def read_dict(text, sep =',', map_sep=':', test_number = True):
    if not text or text.upper() == 'NONE':
        return None
    list_maps = [x.strip() for x in text.split(sep)]
    map_dict = dict()
    for cm in list_maps :
        key = cm.split(map_sep)[0].strip().strip("'\"")
        value = cm.split(map_sep)[1].strip().strip("'\"")
        if test_number :
            key = number_test(key)
            value = number_test(value)
        map_dict[key] = value
    return map_dict

#### READ DICT of DICT
# map_values : column1: {from_value: to_value}, column2: {from_value: to_value}
# 1. split outer_sep
# 2. split first occurrance of map_sep from rest - o_key
# 3. split rest by sep -> dict
def read_dict_of_dict(text, sep=',', map_sep=':', outer_sep = ';', test_number = True) :
    if not text or text.upper() == 'NONE':
        return None
    #if not outer_sep in text :  Produces Errors
    #    outer_sep = sep
    o_dict = dict()
    o_list = [x.strip() for x in text.split(outer_sep)]
    for ov in o_list:
        o_key, i_map = [x.strip() for x in ov.split(map_sep,1)]
        o_key = o_key.strip().strip("'\"")
        i_list = [x.strip() for x in i_map.split(sep)]
        i_dict = dict()
        for iv in i_list :
            i_key,value = [x.strip() for x in iv.split(map_sep)]
            i_key = i_key.strip().strip("(){}").strip().strip("'\"")
            value = value.strip().strip("(){}").strip().strip("'\"")
            if value.upper() == 'NAN' or value.upper() == 'NULL' or value.upper() == 'NONE':
                value = None
            if test_number :
                i_key = number_test(i_key)
                value = number_test(value)
            i_dict[i_key] = value
        o_dict[o_key] = i_dict
    return o_dict

#### READ JSON
def read_json(text) :
    if not text or text.upper() == 'NONE':
        return None
    j = json.loads(text)
    return j

#### READ Relation
def read_relations(text,sep=',',relation_map=None):
    if not text or text.upper() == 'NONE':
        return None
    if not relation_map :
        relation_map = {'!=': '!', '~':'!', '==': '=', '>=': '≥', '=>': '≥', '<=': '≤', '=<': '≤'}

    for key,value in relation_map.items() :
        text = text.replace(key,value)
    text_list = text.split(sep)

    print (text_list)

    relation_list = list()
    for selection in text_list:
        m = re.match(u'(.+)\s*([<>!≤≥=]+)\s*(.+)', selection)
        if m:
            left = m.group(1).strip().strip('"').strip("'")
            right = float(m.group(3).strip())
            relation = m.group(2).strip()
            relation_list.append([left,relation,right])
        else:
            raise ValueError('Could not parse relation statement: ' + selection)
    return relation_list

def number_test(str) :
    if str :
        if re.match('True',str) :
            return True
        elif re.match('False', str):
            return False
        elif re.match('None',str) :
            return None
        elif str.isdigit() :
            return int(str)
        else :
            try :
                f = float(str)
                return f
            except :
                return str
    else :
        return str

##############################
### MAIN
##############################
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    #tflogger.setLevel(logging.DEBUG)
    tflogger.info('Main')
    ### list

    text_1 = "'1'"

    text = "'Hello', 'a list', with , 5, 5.6, separated by , me"
    not_text = "Not Mercedes, Renault, Citroen, Peugeaut, 'Rolls Royce'"
    list2 = ['Mercedes', 'Audi', 'VW', 'Skoda', 'Renault', 'Citroen', 'Peugeot', 'Rolls Royce']
    print('Value: ' + str(read_value(text_1)))
    print('Value Number: ' + str(read_value(text_1,test_number=True)))
    print('Not: ' + str(read_list(not_text, list2)))
    print('None :' + str(read_list('')))
    print('None (literally):' + str(read_list('None')))
    print('List: ' + str(read_list(text)))

    print('ALL: ',str(read_list('ALL',list2)))

    ### value lists
    value_str = "'Mercedes':expensive, German, respectable; Audi:'sportive, German, technology-advanced'; \
                    VW : 'people', 9,'solid', No1; Citroen:cool, Fantomas, CV2, elastic ; 'Rolls Rocye': royal,  \
                    British, 'very expensive', black, 8, True"

    print('Value lists: ' + str(read_dict_of_list(value_str)))

    ### map
    maplist = "'Mercedes':expensive, Audi:'sportive', VW : 'people', Citroen:cool, 'Rolls Rocye': royal, 'Cars':6, 'eco':'False'"
    print('Map :' + str(read_dict(maplist)))

    ### maps of maps
    dictdict = "'High Class':{'Mercedes':expensive, 'BWM':'sporty};'Sport Class':{ Porsche:'None',Ferrari:special}; \
                'Middle Class':{VW : 'people','Renault':'fashionable',Citroen:'classy, 'Peugeot':'modern'};\
                    'Luxury Class':{'Rolls Rocye': royal, Bentley:'rare'}; 'All:{4:8.9,6:90,7:7}"
    print('Maps of maps :' + str(read_dict_of_dict(dictdict)))


    ### json
    json_text = "{\"Luxury Class\": {\"Mercedes\":\"expensive\",\"Rolls Rocye\": \"royal\"}, \"High Middle Class\":{\"Audi\":\"sportiv\"}, \"Middle Class\": {}, \
                \"Middle Class\" : {\"Citroen\":\"cool\",\"VW\" : \"people\" }}"
    print('JSON: ' + str(read_json(json_text)))


    ### comparison
    comparison = ' anna > 1.70, norbert != 900, cindy <= 1.65'
    print('Comparison: ' + str(read_relations(comparison)))

