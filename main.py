"""

    """

import json
import re
import shutil
from pathlib import Path

import pandas as pd
from mirutil.pathes import get_all_subdirs
from mirutil.pathes import has_subdir


class Params :
    _pth = '/Users/mahdi/Library/CloudStorage/OneDrive-khatam.ac.ir/Heidari Data/V2'
    root_dir = Path(_pth)

p = Params()

class ColName :
    path = 'path'
    fdir = 'final_dir'
    relpath = 'relative_path'
    nwrp = 'new_relative_path'
    nwfp = 'new_full_path'
    has_1js = 'has_1js'
    lj = 'list_of_jsons'
    nj = 'number_of_jsons'
    jp = 'json_path'
    ls = 'list_dir'
    nfs = 'number_of_files'
    fns = 'file_stems'
    dp = 'data_path'
    ndn = 'new_data_name'
    ndp = 'new_data_path'

c = ColName()

def list_json(dirp: Path) :
    return list(dirp.glob('*.json'))

conv = {
        "Single Line Expression" : "dcs" ,
        "Start End Column"       : "tcol" ,
        "Start"                  : "start" ,
        "End"                    : "end" ,
        "Frequency"              : "freq" ,
        }

conv1 = {
        "dcs"   : None ,
        "tcol"  : None ,
        "start" : None ,
        "end"   : None ,
        "freq"  : None ,
        "cols"  : None ,
        }

def fix_jsons(jsp) :
    with open(jsp , 'r') as fi :
        js = json.load(fi)

    njs = {}
    for k , v in conv.items() :
        if k in js.keys() :
            njs[v] = js[k]

    for k in conv1.keys() :
        if k not in njs.keys() :
            njs[k] = None

    with open(jsp , 'w') as fo :
        json.dump(njs , fo , indent = 4)

def list_dir(dirp: Path) :
    lo = list(dirp.glob('*'))
    lo = [i for i in lo if i.name != '.DS_Store']
    return lo

def rm_samples(ls) :
    for el in ls :
        if el.stem == 'Sample' or re.match(r'.+-[sS]ample' , el.stem) :
            el.unlink()

def rm_not_dirs(ls) :
    for el in ls :
        if not el.is_dir() :
            el.unlink()
            print(el)

def main() :
    pass

    ##
    while True :
        subs = get_all_subdirs(p.root_dir)

        df = pd.DataFrame()
        df[c.path] = list(subs)

        df[c.relpath] = df[c.path].apply(lambda x : x.relative_to(p.root_dir))

        df[c.nwrp] = df[c.relpath].astype('string').str.replace('\s' , '-')
        df[c.nwrp] = df[c.nwrp].str.replace('-+' , '-')
        df[c.nwrp] = df[c.nwrp].str.replace('\(' , '')
        df[c.nwrp] = df[c.nwrp].str.replace('\)' , '')
        df[c.nwfp] = df[c.nwrp].apply(lambda x : p.root_dir / x)

        try :
            _ = df.apply(lambda x : x[c.path].rename(x[c.nwfp]) , axis = 1)
            if df[c.path].eq(df[c.nwfp]).all() :
                break
        except FileNotFoundError as e :
            print(e)
            pass

    ##
    df[c.fdir] = ~ df[c.path].apply(has_subdir)
    ##
    df[c.ls] = df[c.path].apply(list_dir)
    ##
    msk = ~ df[c.fdir]

    _ = df.loc[msk , c.ls].apply(rm_not_dirs)
    ##
    df1 = df[df[c.fdir]]
    ##
    df1[c.lj] = df1[c.path].apply(list_json)
    df1[c.nj] = df1[c.lj].apply(len)
    ##
    msk = ~ df1[c.nj].eq(1)
    print(len(msk[msk]))

    pt1 = '/Users/mahdi/Dropbox/PycharmProjects/fix-meta-jsons-HData/meta.json'
    pth = Path(pt1)
    _ = df1.loc[msk , c.path].apply(
            lambda x : shutil.copy(pth , x / 'mata.json')
            )

    ##
    df1[c.lj] = df1[c.path].apply(list_json)
    df1[c.nj] = df1[c.lj].apply(len)

    df1[c.jp] = df1[c.lj].apply(lambda x : x[0])
    ##
    _ = df1[c.jp].apply(lambda x : x.rename(x.parent / 'meta.json'))
    ##
    df1[c.lj] = df1[c.path].apply(list_json)
    df1[c.jp] = df1[c.lj].apply(lambda x : x[0])
    ##
    _ = df1[c.jp].apply(fix_jsons)

    ##
    df1[c.ls] = df1[c.path].apply(list_dir)
    df1[c.nfs] = df1[c.ls].apply(len)

    ##
    df1[c.fns] = df1[c.ls].apply(lambda x : [i.name for i in x])

    ##
    _ = df1[c.ls].apply(rm_samples)

    ##
    df1[c.ls] = df1[c.path].apply(list_dir)
    df1[c.nfs] = df1[c.ls].apply(len)
    df1[c.fns] = df1[c.ls].apply(lambda x : [i.name for i in x])

    ##
    df1[c.dp] = df1[c.ls].apply(
            lambda x : x[0] if x[0].name != 'meta.json' else x[1]
            )

    ##
    df1[c.ndn] = df1[c.dp].apply(lambda x : x.name)
    df1[c.ndn] = df1[c.ndn].str.replace('\s' , '-')
    df1[c.ndn] = df1[c.ndn].str.replace('-+' , '-')
    df1[c.ndn] = df1[c.ndn].str.replace('\(' , '')
    df1[c.ndn] = df1[c.ndn].str.replace('\)' , '')

    ##
    df1[c.ndp] = df1.apply(lambda x : x[c.dp].parent / x[c.ndn] , axis = 1)

    ##
    _ = df1.apply(lambda x : x[c.dp].rename(x[c.ndp]) , axis = 1)

    ##

    ##

    ##

##
if __name__ == "__main__" :
    main()
    print("Done!")
