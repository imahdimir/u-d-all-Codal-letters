""" combines old (82-89) data with new(after 89) data

    """

##


import ast

import pandas as pd
from githubdata import GithubData
from mirutil import utils as mu
from mirutil.df_utils import save_as_prq_wo_index as sprq
from persiantools import digits


class RepoUrls :
    src = 'https://github.com/imahdimir/raw-d-all-Codal-letters'
    targ = 'https://github.com/imahdimir/d-all-Codal-Letters'
    cur = 'https://github.com/imahdimir/b-d-all-Codal-letters'

ru = RepoUrls()

class RawDataStems :
    old = '82-89.prq'
    new = 'after-89.prq'

rd = RawDataStems()

class ColNames :
    tran = 'TracingNo'
    ctic = 'CodalTicker'
    supv = 'SuperVision'
    unds = 'UnderSupervision'
    addinfo = 'AdditionalInfo'
    reasons = 'Reasons'
    supvund = supv + '.' + unds
    supvadd = supv + '.' + addinfo
    supvreas = supv + '.' + reasons

cn = ColNames()

def make_old_columns_compatible_to_new(df) :
    cols_map = {
            'نماد'    : 'Symbol' ,
            'شرکت'    : 'CompanyName' ,
            'اطلاعیه' : 'Title' ,
            'انتشار'  : 'PublishDateTime' ,
            'پیوست'   : 'AttachmentUrl' ,
            'pgn'     : 'opgn' ,
            }
    df = df.rename(columns = cols_map)
    return df

def drop_some_cols(df) :
    cols2drop = {
            'pgn'  : None ,
            'opgn' : None ,
            }
    df = df.drop(columns = cols2drop.keys())
    return df

def rename_cols(df) :
    cols_map = {
            'Symbol' : cn.ctic ,
            }
    df = df.rename(columns = cols_map)
    return df

def fix_codalticker_col(df) :
    _cn = cn.ctic

    df[_cn] = df[_cn].str.strip()
    df[_cn] = df[_cn].str.replace('^-$' , '')

    msk = df[_cn].eq('')
    df.loc[msk , _cn] = None

    msk = df[_cn].notna()
    df.loc[msk , _cn] = df[_cn].astype(str)
    return df

def fix_tracing_no_col(df) :
    df[cn.tran] = df[cn.tran].astype('Int64')

    msk = df[cn.tran].notna()
    df.loc[msk , cn.tran] = df[cn.tran].astype(str)
    return df

def fix_datetime_cols(df) :
    for cn in ['SentDateTime' , 'PublishDateTime'] :
        msk = df[cn].notna()
        df.loc[msk , cn] = df.loc[msk , cn].apply(lambda x : digits.fa_to_en(x))

        df[cn] = df[cn].str.replace('/' , '-')
    return df

def fix_undersupervision_col(df) :
    cn = 'UnderSupervision'
    map_f = {
            0 : False ,
            1 : True ,
            }

    df[cn] = df[cn].astype("Int8")
    df[cn] = df[cn].map(map_f , na_action = 'ignore')

    msk = df[cn].isna()
    df.loc[msk , cn] = None

    assert df[cn].isin([True , False , None]).all()

    df[cn] = df[cn].astype("boolean")
    return df

def make_supervision_col_3_distinct_cols(df) :
    _cn = cn.supv

    msk = df[_cn].notna()
    df.loc[msk , _cn] = df.loc[msk , _cn].apply(ast.literal_eval)

    assert df.loc[msk , _cn].apply(lambda x : len(x.keys()) == 3).all()

    col_key = {
            cn.supvund  : (cn.unds , 'Int8') ,
            cn.supvadd  : (cn.addinfo , str) ,
            cn.supvreas : (cn.reasons , str)
            }

    for col , ky in col_key.items() :
        df.loc[msk , col] = df.loc[msk , _cn].apply(lambda x : x[ky[0]])

        msk = df[col].notna()
        df.loc[msk , col] = df.loc[msk , col].astype(ky[1])
    df = df.drop(columns = _cn)
    return df

def make_bool_cols_as_nullable_boolean_type(df) :
    for cn in ['HasHtml' , 'IsEstimate' , 'HasExcel' , 'HasPdf' , 'HasXbrl' ,
               'HasAttachment'] :
        df[cn] = df[cn].astype('boolean')
    return df

def fix_cols_order(df) :
    ord = {
            "TracingNo"        : None ,
            cn.ctic            : None ,
            "CompanyName"      : None ,
            "LetterCode"       : None ,
            "Title"            : None ,
            "SentDateTime"     : None ,
            "PublishDateTime"  : None ,
            "UnderSupervision" : None ,
            cn.supvund         : None ,
            cn.supvadd         : None ,
            cn.supvreas        : None ,
            "IsEstimate"       : None ,
            "TedanUrl"         : None ,
            "HasHtml"          : None ,
            "Url"              : None ,
            "HasAttachment"    : None ,
            "AttachmentUrl"    : None ,
            "HasExcel"         : None ,
            "ExcelUrl"         : None ,
            "HasPdf"           : None ,
            "PdfUrl"           : None ,
            "HasXbrl"          : None ,
            "XbrlUrl"          : None ,
            }

    df = df[list(ord.keys())]
    return df

def main() :

    pass

    ##

    rp_src = GithubData(ru.src)
    rp_src.clone()
    ##
    dfop = rp_src.local_path / rd.old
    dfp = rp_src.local_path / rd.new
    ##
    dfo = pd.read_parquet(dfop)
    df = pd.read_parquet(dfp)
    ##
    dfo = make_old_columns_compatible_to_new(dfo)
    ##
    df = pd.concat([df , dfo])
    ##
    df = df.reset_index(drop = True)
    ##
    df = drop_some_cols(df)
    ##
    df = rename_cols(df)
    ##
    df = fix_codalticker_col(df)
    ##
    df = fix_tracing_no_col(df)
    ##
    df = fix_datetime_cols(df)
    ##
    df = fix_undersupervision_col(df)
    ##
    df = make_supervision_col_3_distinct_cols(df)
    ##
    df = make_bool_cols_as_nullable_boolean_type(df)
    ##
    df = fix_cols_order(df)
    ##

    rp_targ = GithubData(ru.targ)
    rp_targ.clone()
    ##
    ofp = rp_targ.data_fp
    ##
    sprq(df , ofp)
    ##
    tokp = '/Users/mahdi/Dropbox/tok.txt'
    tok = mu.get_tok_if_accessible(tokp)
    ##
    msg = 'Data Updated by: '
    msg += ru.cur

    rp_targ.commit_and_push(msg , user = rp_targ.user_name , token = tok)

    ##


    rp_src.rmdir()
    rp_targ.rmdir()


    ##


    ##

##
if __name__ == '__main__' :
    main()

##

##