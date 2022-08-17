##

"""

    """

##

import asyncio
from pathlib import Path
from pathlib import PurePath

import nest_asyncio
import pandas as pd
import requests
import xmltodict
from aiohttp import client_exceptions
from aiohttp import ClientSession

nest_asyncio.apply()  # Run this line in cell mode to code work

outpn = Path('dta/after_89.prq')

class ReqParams :
  def __init__(self) :
    # https://search.codal.ir/api/search/v2/q?&Audited=true&AuditorRef=-1&Category=-1&Childs=true&CompanyState=-1&CompanyType=-1&Consolidatable=true&IsNotAudited=false&Length=-1&LetterType=-1&Mains=true&NotAudited=true&NotConsolidatable=true&PageNumber=1&Publisher=false&TracingNo=-1&search=false
    self.search_url = "https://search.codal.ir/api/search/v2/q"
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

def rtrn_clustrs_inds(iterable_obj , clustersize = 100) :
  ndiv = len(iterable_obj) // clustersize
  inds = [x * clustersize for x in range(0 , ndiv + 1)]
  if len(inds) > 1 :
    if inds[-1] != len(iterable_obj) :
      inds.append(inds[-1] + len(iterable_obj) % clustersize)
  else :
    inds = [0 , len(iterable_obj)]
    if inds == [0 , 0] :
      inds = [0]
  print(inds)
  return inds

async def dl_pg_src(params) :
  async with ClientSession() as session :
    async with session.get(rq.search_url ,
                           params = params ,
                           verify_ssl = False , ) as resp :
      # print(f"Got Resp")
      ojs = await resp.json()
      return ojs

async def scrp_task(param , pgn) :
  rt = await dl_pg_src(param)
  lett = rt['Letters']
  df = pd.DataFrame(data = lett)
  df['pgn'] = pgn
  return df

async def pgs_read_main(params , pgn) :
  tasks = [scrp_task(x , y) for x , y in zip(params , pgn)]
  out = await asyncio.gather(*tasks)
  return out

def main() :

  pass

  ##


  resp = requests.get(rq.search_url , headers = rq.headers , timeout = 50)

  print(resp)
  assert resp.status_code == 200

  ##
  resp_js = xmltodict.parse(resp.text)
  l2_js = resp_js['SearchReportListDto']

  ##
  total_pgs = int(l2_js["Page"])
  print(f'Total pages are {total_pgs}')

  ##
  tbl_rows = l2_js['Letters']['CodalLetterHeaderDto']
  dft = pd.DataFrame(data = tbl_rows)

  ##
  dft['SuperVision'] = dft['SuperVision'].astype(str)
  dft.to_parquet('dft.prq' , index = False)

  ##
  if outpn.exists() :
    df = pd.read_parquet(outpn)
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

  print(all_params[-1]['PageNumber'])

  ##
  clstrs = rtrn_clustrs_inds(all_params , 20)

  ##
  _i = 0
  _i_1 = 0
  if len(clstrs) > 2 :
    while (_i != len(clstrs) - 1 - 1) :
      try :
        for i in range(_i , len(clstrs) - 1) :
          strt_idx = clstrs[i]
          end_idx = clstrs[i + 1]
          print(f'{strt_idx} to {end_idx}')

          params = all_params[strt_idx : end_idx]
          pgns = all_pgns[strt_idx : end_idx]

          out = asyncio.run(pgs_read_main(params , pgns))

          df = pd.concat([df , *out])

          df['SuperVision'] = df['SuperVision'].astype(str)
          df = df.drop_duplicates()
          df.to_parquet(outpn , index = False)

          _i = i

          # break

      except (client_exceptions.ClientOSError ,
              client_exceptions.ContentTypeError) as er :
        print(er)
        _i_1 += 1
        if _i_1 == 2 :
          _i_1 = 0
          _i += 1

      except KeyboardInterrupt :
        pass

  else :
    for _ in range(0 , 3) :
      try :
        for i in range(0 , len(clstrs) - 1) :
          strt_idx = clstrs[i]
          end_idx = clstrs[i + 1]
          print(f'{strt_idx} to {end_idx}')

          params = all_params[strt_idx : end_idx]
          pgns = all_pgns[strt_idx : end_idx]

          out = asyncio.run(pgs_read_main(params , pgns))

          df = pd.concat([df , *out])

          df['SuperVision'] = df['SuperVision'].astype(str)
          df = df.drop_duplicates()
          df.to_parquet(outpn , index = False)

      except (client_exceptions.ClientOSError ,
              client_exceptions.ContentTypeError) as er :
        print(er)
        pass

      except KeyboardInterrupt :
        pass

  ##
  df = df.drop_duplicates(subset = df.columns.difference(['pgn']))
  df.to_parquet(outpn , index = False)

##


if __name__ == "__main__" :
  main()