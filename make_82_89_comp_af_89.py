"""

    """

import pandas as pd
from giteasy import GitHubRepo
from mirutil.df import save_as_prq_wo_index as sprq

import ns
from main import RDFN


gdu = ns.GDU()
c = ns.DAllCodalLetters()
rdfn = RDFN()

def make_old_columns_compatible_to_new(df) :
    cols_map = {
            'نماد'    : c.CodalTicker ,
            'شرکت'    : c.CompanyName ,
            'اطلاعیه' : c.Title ,
            'انتشار'  : c.PublishDateTime ,
            'پیوست'   : c.AttachmentUrl ,
            }
    df = df.rename(columns = cols_map)
    return df

def main() :
    pass

    ##
    ghr = GitHubRepo(gdu.rd)
    ghr.clone_overwrite()

    ##
    df_fp = ghr.local_path / rdfn.bf89
    df = pd.read_parquet(df_fp)

    ##
    df = make_old_columns_compatible_to_new(df)

    ##
    df = df.drop(columns = ['pgn'])

    ##
    df_sfp = ghr.local_path / rdfn.bf89c
    sprq(df , df_sfp)

    ##
    msg = 'added comaptbile form of before 89 data'
    ghr.commit_and_push(msg)

##


if __name__ == '__main__' :
    main()
