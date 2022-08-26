##

"""
combines old (82-89) data with new(after 89) data
    """

import ast
from pathlib import Path

import pandas as pd
from githubdata import GithubData
from persiantools import digits


acod_rp_url = 'https://github.com/imahdimir/d-all-Codal-Letters'
cur_rp_url = 'https://github.com/imahdimir/get-all-Codal-Letters'

oldfp = Path('dta/82_89.prq')
newfp = Path('dta/after_89.prq')

tran = 'TracingNo'
codtic = 'CodalTicker'
supv = 'SuperVision'
unds = 'UnderSupervision'
addinfo = 'AdditionalInfo'
reasons = 'Reasons'
supvund = supv + '.' + unds
supvadd = supv + '.' + addinfo
supvreas = supv + '.' + reasons

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
      'Symbol' : codtic ,
      }

  df = df.rename(columns = cols_map)

  return df

def fix_codalticker_col(df):
  cn = codtic

  df[cn] = df[cn].str.strip()
  df[cn] = df[cn].str.replace('^-$', '')

  msk = df[cn].eq('')
  df.loc[msk, cn] = None

  df[cn] = df[cn].astype(str)

  return df


def fix_tracing_no_col(df) :
  df[tran] = df[tran].astype('Int64')

  msk = df[tran].notna()
  df.loc[msk , tran] = df[tran].astype(str)

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
  cn = supv

  msk = df[cn].notna()
  df.loc[msk , cn] = df.loc[msk , cn].apply(ast.literal_eval)

  assert df.loc[msk , cn].apply(lambda x : len(x.keys()) == 3).all()

  col_key = {
      supvund  : (unds , 'Int8') ,
      supvadd  : (addinfo , str) ,
      supvreas : (reasons , str)
      }

  for col , ky in col_key.items() :
    df.loc[msk , col] = df.loc[msk , cn].apply(lambda x : x[ky[0]])
    df[col] = df[col].astype(ky[1])

  df = df.drop(columns = cn)

  return df

def make_bool_cols_as_nullable_boolean_type(df) :
  for cn in ['HasHtml' , 'IsEstimate' , 'HasExcel' , 'HasPdf' , 'HasXbrl' ,
             'HasAttachment'] :
    df[cn] = df[cn].astype('boolean')

  return df

def fix_cols_order(df) :
  ord = {
      "TracingNo"        : None ,
      codtic             : None ,
      "CompanyName"      : None ,
      "LetterCode"       : None ,
      "Title"            : None ,
      "SentDateTime"     : None ,
      "PublishDateTime"  : None ,
      "UnderSupervision" : None ,
      supvund            : None ,
      supvadd            : None ,
      supvreas           : None ,
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

  dfo = pd.read_parquet(oldfp)
  df = pd.read_parquet(newfp)

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
  df.to_parquet('dta/com.prq' , index = False)

  ##
  acod_rp = GithubData(acod_rp_url)
  acod_rp.clone()

  ##
  ofp = acod_rp.data_filepath

  ##
  df.to_parquet(ofp , index = False)

  ##
  msg = 'Data Updated by: '
  msg += cur_rp_url

  acod_rp.commit_push(msg)

  ##

  acod_rp.rmdir()


##