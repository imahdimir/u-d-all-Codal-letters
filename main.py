"""

    """

from pathlib import Path

import pandas as pd
from giteasy import GitHubRepo
from githubdata import GitHubDataRepo
from mirutil.classs import return_not_special_variables_of_class as rnsvoc
from mirutil.df import save_as_prq_wo_index as sprq
from mirutil.ns import update_ns_module as unm
from persiantools import digits


unm()
import ns


gdu = ns.GDU()
c = ns.DAllCodalLetters()

class RDFN :
    bf89 = '82-89.prq'
    af89 = 'after-89.prq'
    bf89c = '82-89-compatible.prq'

rd = RDFN()

class ColName :
    supv = "SuperVision"

coln = ColName()

class Supv :
    und = 'UnderSupervision'
    addinfo = 'AdditionalInfo'
    reasons = 'Reasons'

supv = Supv()

def main() :
    pass

    ##
    from _1_after_89 import main as af89m

    af89m()

    ##
    ghr_rd = GitHubRepo(gdu.rd)

    af89_fp = ghr_rd.local_path / rd.af89
    bf89_fp = ghr_rd.local_path / rd.bf89c

    ##
    da89 = pd.read_parquet(af89_fp)
    db89 = pd.read_parquet(bf89_fp)

    ##
    da89 = da89.drop(columns = ['pgn'])
    da89 = da89.rename(columns = {
            'Symbol' : c.CodalTicker
            })

    ##
    df = pd.concat([da89 , db89])

    ##
    def fix_codalticker_col(df) :
        col = c.CodalTicker

        df[col] = df[col].str.strip()
        df[col] = df[col].str.replace('^-$' , '')

        msk = df[col].eq('')
        df.loc[msk , col] = None

        df[col] = df[col].astype('string')
        return df

    df = fix_codalticker_col(df)

    ##
    def fix_tracing_no_col(df) :
        df[c.TracingNo] = df[c.TracingNo].astype('Int64')
        df[c.TracingNo] = df[c.TracingNo].astype('string')
        return df

    df = fix_tracing_no_col(df)

    ##
    def fix_datetime_cols(df) :
        for col in [c.SentDateTime , c.PublishDateTime] :
            msk = df[col].notna()
            df.loc[msk , col] = df.loc[msk , col].apply(digits.fa_to_en)
            df[col] = df[col].str.replace('/' , '-')
        return df

    df = fix_datetime_cols(df)

    ##
    def fix_undersupervision_col(df) :
        col = c.UnderSupervision
        map_f = {
                0 : False ,
                1 : True ,
                }

        df[col] = df[col].astype("Int8")
        df[col] = df[col].map(map_f , na_action = 'ignore')

        msk = df[col].isna()
        df.loc[msk , col] = None

        assert df[col].isin([True , False , None]).all()

        df[col] = df[col].astype("boolean")
        return df

    df = fix_undersupervision_col(df)

    ##
    def make_supervision_col_3_distinct_cols(df) :
        _cn = coln.supv

        msk = df[_cn].notna()
        df.loc[msk , _cn] = df.loc[msk , _cn].apply(lambda x : eval(str(x)))

        assert df.loc[msk , _cn].apply(lambda x : len(x.keys()) == 3).all()

        col_key = {
                c.SuperVision_UnderSupervision : (supv.und , 'Int8') ,
                c.SuperVision_AdditionalInfo   : (supv.addinfo , 'string') ,
                c.SuperVision_Reasons          : (supv.reasons , 'string') ,
                }

        for col , ky in col_key.items() :
            df.loc[msk , col] = df.loc[msk , _cn].apply(lambda x : x[ky[0]])
            df[col] = df[col].astype(ky[1])

        df = df.drop(columns = _cn)
        return df

    df = make_supervision_col_3_distinct_cols(df)

    ##
    def make_bool_cols_as_nullable_boolean_type(df) :
        cols = {
                c.HasHtml       : None ,
                c.IsEstimate    : None ,
                c.HasExcel      : None ,
                c.HasPdf        : None ,
                c.HasXbrl       : None ,
                c.HasAttachment : None ,
                }

        for cn in cols.keys() :
            df[cn] = df[cn].astype('boolean')

        return df

    df = make_bool_cols_as_nullable_boolean_type(df)

    ##
    def fix_cols_order(df) :
        cols = rnsvoc(ns.DAllCodalLetters).values()
        print(cols)
        df = df[list(cols)]
        return df

    df = fix_cols_order(df)

    ##
    df = df.drop_duplicates()

    ##
    df = df.sort_values(c.PublishDateTime , ascending = False)

    ##
    ghrt = GitHubDataRepo(gdu.trg)
    ghrt.clone_overwrite()

    ##
    sprq(df , ghrt.data_fp)

    ##
    msg = f'data got updated, rows: {len(df)}'
    ghrt.commit_and_push(msg)

    ##
    ghr_rd.rmdir()
    ghrt.rmdir()

##


if __name__ == '__main__' :
    main()
    print(f'{Path(__file__).name} done!')
