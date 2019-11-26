import pandas as pd
import numpy as np
import logging
import re
import json


EXAMPLE_ROWS = 5

def unique_values(df) :
    num_unique_vals = dict()
    for col in df.columns :
        num_unique_vals[col] = len(df[col].unique())
        if num_unique_vals[col] <= 2 and df[col].dtypes == np.object:
            print(df[col].unique())
    return  num_unique_vals

def get_value_list(param_str,val_list) :
    # only a list with a leading modifier is needed
    param_str_clean = param_str.replace(':', '').replace('=', '')
    # Test for ALL
    result = re.match(r'^([Aa][Ll][Ll])\s*$', param_str_clean)
    if result :
        return val_list
    # Test for NOT
    result = re.match(r'^([Nn][Oo][Tt])(.+)', param_str_clean)
    if result and result.group(1).upper() == 'NOT':
        exclude_values = [x.strip().strip("'").strip('"') for x in result.group(2).split(',')]
        ret_val = [x for x in val_list if x not in exclude_values]
    else:
        ret_val = [x.strip().strip("'").strip('"') for x in param_str_clean.split(',')]
    return ret_val

def process(df_msg):

    prev_att = df_msg.attributes
    df = df_msg.body
    if not isinstance(df,pd.DataFrame) :
        raise TypeError('Message body does not contain a pandas DataFrame')

    att_dict = dict()
    att_dict['config'] = dict()

    ###### start of doing calculation

    att_dict['prev_number_columns'] = df.shape[1]
    att_dict['prev_number_rows'] = df.shape[0]

    if api.config.remove_duplicates_cols and not api.config.remove_duplicates_cols.upper() == 'NONE':
        test_cols = get_value_list(api.config.remove_duplicates_cols,df.columns)
        df.drop_duplicates(subset=test_cols,inplace=True)
        logging.debug('#Dropped duplicates: {} - {} = {}'.format(att_dict['prev_number_rows'],df.shape[0], \
                                                                 att_dict['prev_number_rows']- df.shape[0]))
    att_dict['config']['remove_duplicates_cols'] = api.config.remove_duplicates_cols

    if api.config.zero_to_null :
        for col in df.columns :
            if df[col].dtype == np.object :
                df.loc[df[col] == '0',col] = np.nan
    att_dict['config']['zero_to_null'] = str(api.config.zero_to_null)

    if api.config.yes_no_to_num :
        for col in df.columns :
            if df[col].dtype == np.object :
                df[col] = df[col].str.upper()
                vals = [x for x in df.loc[df[col].notnull(),col].unique()]
                if len(vals) == 1 and vals[0] == 'YES' :
                    df.loc[df[col].notnull(),col] = 1
                    df.loc[df[col].isnull(), col] = 0
                    try :
                        df[col] = df[col].astype('int8')
                    except ValueError :
                        print('Value Error: {}'.format(col))
                        print(df[col].unique())

                if len(vals) == 1 and vals[0] == 'NO' :
                    df.loc[df[col].notnull(),col] = 1
                    df.loc[df[col].isnull(), col] = 0
                    df[col] = df[col].astype('int8')
                if len(vals) == 2 and all( i in vals for i in ['YES','NO']) :
                    df[col].replace(to_replace={'NO':0,'YES':1})
                    df[col] = df[col].astype('int8')
    att_dict['config']['yes_no_to_boolean'] = str(api.config.yes_no_to_num)

    # if all values of column == 0 then NaN
    if api.config.all_constant_to_NaN :
        for col in df.columns:
            unique_vals = df[col].unique()
            if len(unique_vals) == 1  :
                df[col] = np.nan
    att_dict['config']['all_constant_to_NaN'] = str(api.config.all_constant_to_NaN)

    # remove rare value rows with quantile
    if api.config.rare_value_cols and not api.config.rare_value_cols.upper() == 'NONE':
        logging.debug('quantile')
        test_cols = get_value_list(api.config.rare_value_cols, df.columns)

        # drop rare values by quantile
        if api.config.rare_value_quantile > 0 :
            if not api.config.rare_value_quantile >= 0 and api.config.rare_value_quantile < 1:
                raise ValueError('Quantile value range: [0,1[, not {}'.format(api.config.rare_value_quantile))
            for col in test_cols:
                unique_num = len(df[col].unique())
                val_num = df[col].count()
                ratio = df[col].count()/len(df[col].unique())
                threshold = df[col].count()/len(df[col].unique())*api.config.rare_value_quantile
                value_counts = df[col].value_counts()  # Specific column
                kept_values = value_counts[value_counts > threshold].count()
                if value_counts[value_counts > threshold].count() > 1:
                    to_remove = value_counts[value_counts <= threshold].index
                    if len(to_remove) > 0 :
                        logging.debug('Drop rare value by quantile: Column {} Removed Values {} '.format(col,len(to_remove)))
                        df[col].replace(to_remove, np.nan, inplace=True)

        # drop rare values by std
        if api.config.rare_value_std > 0:
            for col in df.columns:
                value_counts = df[col].value_counts()
                mean = value_counts.mean()
                threshold = value_counts.mean() - value_counts.std() * api.config.rare_value_std
                if threshold > 1  :
                    to_remove = value_counts[value_counts <= threshold].index
                    if len(to_remove) > 0  :
                        logging.debug('Drop rare value by std: Column {} Removed Values {} '.format(col, len(to_remove)))
                        df[col].replace(to_remove, np.nan, inplace=True)
    att_dict['config']['rare_value_cols'] = api.config.rare_value_cols
    att_dict['config']['rare_value_quantile'] = api.config.rare_value_quantile
    att_dict['config']['rare_value_std'] = api.config.rare_value_std



    # for unique values less then threshold_unique set to 1 else 0
    if api.config.threshold_unique_cols and not api.config.threshold_unique_cols.upper() == 'NONE':
        logging.debug('Threshold unique')
        test_cols = get_value_list(api.config.threshold_unique_cols,df.columns)
        for col in test_cols:
            if df[col].dtype == np.object :
                unique_vals = list(df[col].unique())
                if len(unique_vals) <= api.config.threshold_unique:
                    # test if one of the values is non
                    if np.nan in unique_vals :
                        df.loc[df[col].notnull(),col] = 1
                        df.loc[df[col].isnull(),col] = 0
                    else :
                        raise ValueError('For treshold_unique there needs to be NaN values existing ({} : {})'\
                                         .format(col,unique_vals))
                    df[col] = df[col].astype('int8')

    att_dict['config']['threshold_unique_cols'] = api.config.threshold_unique_cols
    att_dict['config']['threshold_unique'] = api.config.threshold_unique

    # for count values less then threshold_count set to NaN
    if api.config.threshold_freq_cols and not api.config.threshold_freq_cols.upper() == 'NONE':
        logging.debug('Threshold freq')
        test_cols = get_value_list(api.config.threshold_freq_cols, df.columns)
        if api.config.reduce_categoricals_only :
            test_cols = [ot for ot in test_cols if df[ot].dtype==np.object]
        if api.config.threshold_freq < 1:
            api.config.threshold_freq = api.config.threshold_freq * df.shape[0]

        for col in test_cols:
            if df[col].count() < api.config.threshold_freq:
                logging.debug('Threshold_count: Removed column {} (#values {})'.format(col,df[col].count()))
                df[col] = np.nan

    att_dict['config']['threshold_freq_cols'] = api.config.threshold_unique_cols
    att_dict['config']['threshold_freq'] = api.config.threshold_unique

    # removes columns with to many category values that could not be transposed
    if api.config.max_cat_num > 0 and api.config.max_cat_num_cols and not api.config.max_cat_num_cols.upper() == 'NONE':
        test_cols = get_value_list(api.config.max_cat_num_cols,df.columns)
        drop_cols = list()
        for col in test_cols:
            if df[col].dtype == np.object :
                if len(df[col].unique()) > api.config.max_cat_num :
                    drop_cols.append(col)
        df.drop(columns = drop_cols,inplace=True)
    att_dict['config']['max_cat_num'] = api.config.max_cat_num

    # remove cols with only NaN
    if api.config.drop_nan_columns :
        df.dropna(axis='columns',how='all',inplace=True)
    att_dict['config']['drop_nan_columns'] = str(api.config.drop_nan_columns)

    # remove rows with NAN except for dimension cols
    if api.config.drop_nan_rows_cols and not api.config.drop_nan_rows_cols.upper() == 'NONE':
        drop_cols = get_value_list(api.config.drop_nan_rows_cols, df.columns)
        df[drop_cols].dropna(subset=drop_cols, how = 'all',inplace=True)
    att_dict['config']['drop_nan_rows_cols'] = str(api.config.drop_nan_rows_cols)

    print('Cols: {} -> {}   Rows: {} -> {}'.format(att_dict['prev_number_columns'],df.shape[1],
                                                   att_dict['prev_number_rows'], df.shape[0]))

    ###### end of doing calculation


    ##############################################
    #  final infos to attributes and info message
    ##############################################

    if df.empty :
        raise ValueError('DataFrame is empty')

    att_dict['operator'] = 'selectDataFrame'
    att_dict['name'] = prev_att['name']
    att_dict['memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = str(list(df.columns))
    att_dict['number_columns'] = df.shape[1]
    att_dict['number_rows'] = df.shape[0]

    example_rows = EXAMPLE_ROWS if att_dict['number_rows'] > EXAMPLE_ROWS else att_dict['number_rows']
    for i in range(0,example_rows) :
        att_dict['row_'+str(i)] = str([ str(i)[:10].ljust(10) for i in df.iloc[i, :].tolist()])

    return  api.Message(attributes = att_dict,body=df)


'''
Mock pipeline engine api to allow testing outside pipeline engine
'''

class test :
    SIMPLE = 0

actual_test = test.SIMPLE

try:
    api
except NameError:
    class api:

        def set_test(test_scenario):
            df = pd.DataFrame(
                {'icol': [1, 2, 3, 4, 5], 'col 2': [1, 2, 3, 4, 5], 'col3': [100,200,300,400,500]})

            attributes = {'format': 'csv','name':'DF_name'}

            return api.Message(attributes=attributes,body=df)

        def set_config(test_scenario) :
            api.config.zero_to_null = False

        class config:
            zero_to_null = False # Boolean Value
            yes_no_to_num = False
            drop_nan_columns = False
            all_constant_to_NaN = False
            threshold_unique = 0
            threshold_unique_cols = 'None'
            threshold_freq = 0
            threshold_freq_cols = 'None'
            drop_nan_rows_cols = 'None'
            rare_value_quantile = 0
            rare_value_cols = 'None'
            rare_value_std = 0
            max_cat_num = 0
            max_cat_num_cols = "None"
            reduce_categoricals_only = True
            remove_duplicates_cols = 'None'

        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if not isinstance(msg,str) :
                print(msg.body.head(10))
            #else :
            #    print(msg)
            pass

        def set_port_callback(port, callback):
            msg = api.set_test(actual_test)
            api.set_config(actual_test)
            print("Call \"" + callback.__name__ + "\"  messages port \"" + port + "\"..")
            callback(msg)

        def call(msg,config):
            api.config = config
            result = process(msg)
            return result, json.dumps(result.attributes, indent=4)


def interface(msg):
    result = process(msg)
    api.send("outDataFrameMsg", result)
    info_str = json.dumps(result.attributes, indent=4)
    api.send("Info", info_str)


# Triggers the request for every message (the message provides the stock_symbol)
api.set_port_callback("inDataFrameMsg", interface)

