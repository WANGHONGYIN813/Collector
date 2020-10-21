import requests
import json
import sys
import getopt
import base64
import os
from io import BytesIO
from PIL import Image

from PIL import ImageFile
ImageFile.LOAD_TRUNCATED_IMAGES = True


host_path="10.0.24.42:9900"
table_name='dpi_filerevert'
source_id=''

save_flag=False
file_path=''

unquietmode=True

save_dir="tmp_save_dir/"

def HelpInfo():
    print ("-u: pic stroage server url")
    print ("-s: save")


def env_init():
    if save_flag :
        if not os.path.exists(save_dir) :
            os.makedirs(save_dir)


def OptParse(argv):
    global host_path
    global table_name
    global source_id
    global save_flag
    global file_path
    global unquietmode

    try:
        opts, args = getopt.getopt(argv,"hu:t:i:sf:q",["url=","table=","id=","file="])
    except getopt.GetoptError:
        HelpInfo()
        sys.exit()
    for opt, arg in opts:
        if opt == '-h':
            HelpInfo()
            sys.exit()
        elif opt in ("-u", "--url"):
            host_path = arg
        elif opt in ("-t", "--table"):
            table_name = arg
        elif opt in ("-i", "--id"):
            source_id = arg
        elif opt in ("-s"):
            save_flag = True
        elif opt in ("-q"):
            unquietmode = False
        elif opt in ("-f"):
            file_path = arg



def HbaseGet(url):
    headers = {'Accept': 'application/json'}
    r = requests.get(url, headers=headers)
    return r.text

def HbaseCellParse(ret):
    dict_ret = json.loads(ret)
    for i in dict_ret['Row'] :
        filename = str(base64.b64decode(i['key']),'utf-8')
        for c in i['Cell'] : 
            #__file_body = BytesIO(base64.b64decode(c['$']))
            __file_body = base64.b64decode(c['$'])
            file_body = str(__file_body)
            #print('file_body is', file_body)
            #file_body = str(__file_body)
            image = Image.open(BytesIO(__file_body))
            if save_flag :
                print('write ',filename, 'Done')
                s = save_dir + filename
                image.save(s)
            if unquietmode:
                image.show()


def HbaseDataGet(_id):
    url = 'http://' + host_path + '/' + table_name + '/' + _id
    ret = HbaseGet(url)
    HbaseCellParse(ret)



if __name__ == '__main__':

    OptParse(sys.argv[1:])

    env_init()

    if file_path != '':
        for line in open(file_path):
            line=line.strip('\n')
            HbaseDataGet(line)
    else:
        HbaseDataGet(source_id)

