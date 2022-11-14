"""

    """

import asyncio
from pathlib import Path

import pandas as pd
import requests
from aiohttp import ClientSession
from aiohttp.client_exceptions import ClientOSError , ContentTypeError
from giteasy import GitHubRepo
from mirutil.utils import ret_clusters_indices as rci
from main import RDFN

import ns


gdu = ns.GDU()
rdfn = RDFN()

supv = 'SuperVision'

class ReqParams :
    # https://search.codal.ir/api/search/v2/q?&Audited=true&AuditorRef=-1&Category=-1&Childs=true&CompanyState=-1&CompanyType=-1&Consolidatable=true&IsNotAudited=false&Length=-1&LetterType=-1&Mains=true&NotAudited=true&NotConsolidatable=true&PageNumber=1&Publisher=false&TracingNo=-1&search=false
    burl = "https://search.codal.ir/api/search/v2/q"
    params = {
            "Audited"           : "true" ,
            "AuditorRef"        : "-1" ,
            "Category"          : "-1" ,
            "Childs"            : "true" ,
            "CompanyState"      : "-1" ,
            "CompanyType"       : "-1" ,
            "Consolidatable"    : "true" ,
            "IsNotAudited"      : "false" ,
            "Length"            : "-1" ,
            "LetterType"        : "-1" ,
            "Mains"             : "true" ,
            "NotAudited"        : "true" ,
            "NotConsolidatable" : "true" ,
            "PageNumber"        : "1" ,
            "Publisher"         : "false" ,
            "TracingNo"         : "-1" ,
            "search"            : "false" ,
            }
    hdrs = {
            'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
            }

rq = ReqParams()

async def dl_pg_src(params) :
    async with ClientSession(trust_env = True) as ses :
        async with ses.get(rq.burl , params = params) as r :
            return await r.json()

async def scrp_task(param , pgn) :
    rt = await dl_pg_src(param)
    lett = rt['Letters']
    df = pd.DataFrame(data = lett)
    df['pgn'] = pgn
    return df

async def pgs_read_main(params , pgn) :
    tasks = [scrp_task(x , y) for x , y in zip(params , pgn)]
    return await asyncio.gather(*tasks)

def main() :
    pass

    ##
    resp = requests.get(rq.burl , headers = rq.hdrs , timeout = 50)

    print(resp)
    assert resp.status_code == 200
    print(resp.text)

    ##
    js = resp.json()

    ##
    total_pgs = int(js["Page"])
    print(f'Total pages are {total_pgs}')

    ##
    grp = GitHubRepo(gdu.rd)
    grp.clone_overwrite()

    ##
    df_fp = grp.local_path / rdfn.af89

    df = pd.read_parquet(df_fp)
    bfpgs = df['pgn'].unique()

    ##
    pgs_2crawl = set(range(1 , total_pgs + 1)) - set(bfpgs)
    pgs_2crawl = pgs_2crawl.union({total_pgs , total_pgs - 1})

    ##
    all_params = []
    all_pgns = []

    for pgn in pgs_2crawl :
        npar = rq.params.copy()
        npar['PageNumber'] = total_pgs - pgn + 1
        all_params.append(npar)
        all_pgns.append(pgn)

    print(all_params[0]['PageNumber'])

    ##
    clst = rci(all_params , 20)

    ##
    try :
        for se in clst :
            print(se)
            si = se[0]
            ei = se[1]

            params = all_params[si : ei]
            pgns = all_pgns[si : ei]

            out = asyncio.run(pgs_read_main(params , pgns))

            df = pd.concat([df , *out])

            if supv in df.columns :
                df[supv] = df[supv].astype(str)

    except (ClientOSError , ContentTypeError) as er :
        print(er)

    except KeyboardInterrupt :
        pass

    ##
    df = df.drop_duplicates(subset = df.columns.difference(['pgn']))

    ##
    df.to_parquet(df_fp , index = False)

    ##
    grp.commit_and_push(f'{df_fp.name} got updated, rows: {df.shape[0]}')

##


if __name__ == '__main__' :
    main()
    print(f'{Path(__file__).name} Done!')

##


if False :
    pass

    ##
