import pandas as pd 
import argparse
import magic
import re
import ujson
from datetime import datetime


magic_mapper = {
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':{
        'filetype':'excel',
        'func':pd.read_excel},
    'text/csv':{
        'filetype':'csv',
        'func':pd.read_csv},
    'application/json':{
        'filetype':'json',
        'func':pd.read_json},
    'application/x-ndjson':{
        'filetype':'jsonl' ,
        'func':pd.read_json},
}

extent_mapper = {
    r'\.json$' : "json" ,  
    r"\.csv$":"csv"  ,  
    r"\.xlsx$":"excel"  ,  
}

pd.io.json._json.loads = lambda s, *a, **kw: pd.io.json.json_normalize(ujson.loads(s))

def left_joiner(df1 ,df2 , field):
    new_df = pd.merge(df1,df2,how='left',on=field)
    return new_df


def file2df(file):
    """ read file type and conv to dataframe , only support [csv excel json jsonl] """
    obj = magic_mapper.get( magic.from_file(file,mime=True) , None)
    if not obj  : raise Exception('not support filetype ')
    if obj.get('filetype') == "jsonl": return obj.get('func')(file , lines=True)
    return obj.get('func')(file)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Converting json files into csv for Tableau processing')
    parser.add_argument(
        "-f1", "--file1", dest="file1", help="file1 which need to left join ", required=True)
    parser.add_argument(
        "-f2", "--file2", dest="file2", help="file2 which need to left join ", required=True)
    parser.add_argument(
        "-f", "--field", dest="field", help="field", required=True)
    parser.add_argument(
        "-o", "--output", dest="output", help="output , only support json,excel,csv , be carefully extend")
    
    args = parser.parse_args()
    df1 = file2df(args.file1)
    df1.rename(columns=lambda x: str.lower(x), inplace=True)
    df2 = file2df(args.file2)
    df2.rename(columns=lambda x: str.lower(x), inplace=True)

    output_filename = args.output if args.output else f'{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'

    

    for regex in list(extent_mapper.keys()):
        flag = re.search(regex ,output_filename)
        if flag:
            out_filetype = extent_mapper.get(regex )
            break
    else:
        raise Exception('not support filetype extend')
    

    new_df = left_joiner( df1 , df2 , args.field )

    if out_filetype == "excel":
        with pd.ExcelWriter(
            output_filename
        ) as writer:
            new_df.to_excel(writer,index=False)
    elif out_filetype == "csv":
        new_df.to_csv(output_filename)
    elif out_filetype == "json":
        new_df.to_json(output_filename)
