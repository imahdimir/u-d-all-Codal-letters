##

"""

    """

import asyncio
from pathlib import Path

import nest_asyncio
import pandas as pd
import requests
import xmltodict
from aiohttp import client_exceptions as cer
from aiohttp import ClientSession
from mirutil import funcs as mf


nest_asyncio.apply()  # Run this line in cell mode to code work

outfp = Path('dta/after_89.prq')

supv = 'SuperVision'

class ReqParams :
  def __init__(self) :
    # https://search.codal.ir/api/search/v2/q?&Audited=true&AuditorRef=-1&Category=-1&Childs=true&CompanyState=-1&CompanyType=-1&Consolidatable=true&IsNotAudited=false&Length=-1&LetterType=-1&Mains=true&NotAudited=true&NotConsolidatable=true&PageNumber=1&Publisher=false&TracingNo=-1&search=false
    self.burl = "https://search.codal.ir/api/search/v2/q"
    self.params = {
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

    self.headers = {
        'User-Agent'                : 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36' ,
        "Upgrade-Insecure-Requests" : "1" ,
        "DNT"                       : "1" ,
        "Accept"                    : "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8" ,
        "Accept-Language"           : "en-US,en;q=0.5" ,
        "Accept-Encoding"           : "gzip, deflate"
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

  resp = requests.get(rq.burl , headers = rq.headers , timeout = 50)

  print(resp)
  assert resp.status_code == 200

  ##
  resp_js = xmltodict.parse(resp.text)
  l2_js = resp_js['SearchReportListDto']

  ##
  total_pgs = int(l2_js["Page"])
  print(f'Total pages are {total_pgs}')

  ##
  if outfp.exists() :
    df = pd.read_parquet(outfp)
    bfpgs = df['pgn'].unique()
  else :
    df = pd.DataFrame()
    bfpgs = []

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
  segs = mf.return_clusters_indices(all_params , 20)

  ##
  try :
    for se in segs :
      print(se)
      si = se[0]
      ei = se[1]

      params = all_params[si : ei]
      pgns = all_pgns[si : ei]

      out = asyncio.run(pgs_read_main(params , pgns))

      df = pd.concat([df , *out])

      if supv in df.columns :
        df[supv] = df[supv].astype(str)

      df = df.drop_duplicates()
      df.to_parquet(outfp , index = False)

  except (cer.ClientOSError , cer.ContentTypeError) as er :
    print(er)

  except KeyboardInterrupt :
    pass

  ##
  df = df.drop_duplicates(subset = df.columns.difference(['pgn']))
  df.to_parquet(outfp , index = False)

##