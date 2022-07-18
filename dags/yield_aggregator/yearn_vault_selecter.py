import re

import pandas as pd
import requests
from bs4 import BeautifulSoup

url_head = 'https://medium.com/yearn-state-of-the-vaults/the-vaults-at-yearn-9237905ffed3'

headers = {
    "cache-control": "no-cache",
    "cookie": "__cfduid=dbe54bfe7733c89aca06877ea60a9f0311604561678; _apkcombo_gl=us; _ga=GA1.2.1624024244.1604561679; _gid=GA1.2.1645990040.1604561679; _apkcombo_hl=en",
    "pragma": "no-cache",
    "upgrade-insecure-requests": "1",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
    "referer": "https://apkcombo.com/en-in/category/finance/latest-updates/?page=1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36"
}


def parse_contract_address(h):
    a = h.find('a')
    if not a:
        return 'None'
    if 'href' not in a.attrs:
        return 'None'
    href = a.attrs['href']
    print(href)
    # https://etherscan.io/address/0x7047F90229a057C13BF847C0744D646CFb6c9E1A
    contract_address = href.replace('https://etherscan.io/address/', '')
    contract_address = re.sub(r'#.*', '', contract_address)
    return contract_address.lower()


def parse_text(text):
    name = ''
    desc = re.sub(r' \(.*\)', '', text)
    matchObj = re.match(r'.*\((.*)\)', text, re.M | re.I)
    if matchObj:
        name = matchObj.group(1)
    return name, desc


def run():
    res = requests.get(url_head, headers=headers)
    bs = BeautifulSoup(res.text, 'html.parser')
    h1s = bs.find_all('h2')
    vaults = []
    for h in h1s:
        text = h.text
        if 'yVault' not in text:
            continue
        if 'v2' not in text:
            continue
        name, desc = parse_text(h.text)
        contract_address = parse_contract_address(h)
        print('name', name, 'desc', desc, 'contract_address', contract_address)
        vaults.append({
            'contract_address': contract_address,
            'name': name,
            'desc': desc
        })
    df = pd.DataFrame(vaults)
    # df = self.flatten_column(df, ['baseCurrency', 'quoteCurrency', 'timeInterval', 'exchange'])
    df.to_csv('./yearn_vaults_temp.csv', index=False)


if __name__ == '__main__':
    run()
    # print(parse_text('v2 Curve pBTC Pool yVault (yvCurve-pBTC)'))
