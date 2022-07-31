##

"""
old ones: 1382_1389
    """

##

from pathlib import Path
from pathlib import PurePath

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

outpn = Path('82_89.prq')

chrome_options = Options()
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--headless")
driver = webdriver.Chrome(service = Service(ChromeDriverManager().install()) ,
                          options = chrome_options)

def gen_mycodal_link(pgn) :
  return f'https://my.codal.ir/fa/old-statement/?page={pgn}'

def main() :

  pass

  ##


  if outpn.exists() :
    df = pd.read_parquet(outpn)
    bfpgs = df['pgn'].dropna().astype(int).unique()
  else :
    df = pd.DataFrame()
    bfpgs = []

  ##
  total_pgs = 3471

  pgs_2crawl = set(range(1 , total_pgs + 1)) - set(bfpgs)

  ##
  for pgn in pgs_2crawl :
    driver.get(gen_mycodal_link(pgn))
    htmlt = driver.page_source
    dta = pd.read_html(htmlt)
    df0 = dta[0]

    df0['pgn'] = pgn
    print(pgn)

    df = pd.concat([df , df0])

  ##
  df = df.drop_duplicates(df.columns.difference(['pgn']))
  df.to_parquet(outpn , index = False)

##


if __name__ == "__main__" :
  main()
  print(f'{PurePath(__file__).name} Done.')