import xlrd
import json
import sys, getopt
from socket import *
import os
import csv
 
target_file=""
target_ip="0.0.0.0"
target_port=9020

start_row=0

namelistfile="namelist"

namelistline=0
namelistline_flag = False
csv_flag_split_flag = '\t'


def usage():
    print("-a ipaddr ")
    print("-p port")
    print("-f file")
    print("-n namelist file")
    print("-t title line")
    print("-s start lines")
    print("sample : python %s -a %s -p %d -f 1.xlsx -s 12 -n namelist" % (sys.argv[0], target_ip, target_port))
    print("sample : python %s -a %s -p %d -f 1.csv  -t 1" % (sys.argv[0], target_ip, target_port))
 
try:
    opts, args = getopt.getopt(sys.argv[1:], "ha:p:f:s:n:t:c:")

except getopt.GetoptError:
        usage()
        sys.exit()

for op, value in opts:
    if op == "-f":
        target_file = value
    elif op == "-a":
        target_ip = value
    elif op == "-p":
        target_port = int(value)
    elif op == "-s":
        start_row = int(value)-1
    elif op == "-n":
        namelistfile = value
    elif op == "-c":
        csv_flag_split_flag = value
    elif op == "-t":
        namelistline_flag = True
        namelistline = int(value) - 1
    else:
        usage()
        sys.exit()

namelist=[]

if os.path.exists(namelistfile) :
    for line in open(namelistfile):
        namelist.append(line.strip())
elif namelistline_flag == False :
    print("Please give Namelist file : %s " % namelistfile)
    print("Or use -n give Namelist file path")
    print("Or use -h give title line in %d" % target_file)
    sys.exit()


if target_file == "":
    print("use -f Input File Path")
    sys.exit()


def file_extension(path):
    return os.path.splitext(path)[1]


def Csv_file_hander() :

    if namelistline_flag :

        csvfile = open(target_file,"r")
        reader = csv.reader(csvfile)

        num = 0
        for line in reader:
            if num == namelistline:
                itemlist = line[0].split(csv_flag_split_flag)
                for item in itemlist :
                    namelist.append(item)

            num += 1
        csvfile.close() 

    tcp_client_socket = socket(AF_INET,SOCK_STREAM)
    tcp_client_socket.connect((target_ip, target_port))

    csvfile = open(target_file,"r")
    reader = csv.reader(csvfile)

    start = namelistline + 1

    print("NameList :", namelist)
    print("Start %d " % start)

    num = 0
    send_num = 0
    for line in reader:
        if num >= start :
            info = dict()
            itemlist = line[0].split(csv_flag_split_flag)
            i = 0
            for item in itemlist :
                info[namelist[i]] = item
                i += 1

            send_num += 1

            a = json.dumps(info,  ensure_ascii=False)
            #print(a)
            a += '\n'
            senddata=a.encode()
            tcp_client_socket.send(senddata)
        num += 1

    csvfile.close() 
    tcp_client_socket.close()

    print("Send :", send_num)

def Xlsx_file_hander():
    data = xlrd.open_workbook(target_file)
    table = data.sheets()[0]

    nrows = table.nrows
    ncols = table.ncols

    if namelistline_flag :
        row = table.row_values(namelistline)
        for i in range(0, ncols):
            namelist.append(row[i])

    start=start_row
    end=nrows 

    num = 0

    tcp_client_socket = socket(AF_INET,SOCK_STREAM)
    tcp_client_socket.connect((target_ip, target_port))

    print("nrows %d " % nrows)
    print("ncols %d " % ncols)
    print("Start %d " % start)
    print("End   %d " % end)

    print("NameList :", namelist)

    for x in range(start, end):
        info = dict()
        row =table.row_values(x)
        for i in range(0, ncols):
            info[namelist[i]] = row[i]

        a = json.dumps(info,  ensure_ascii=False)
        #print(a)
        a += '\n'
        senddata=a.encode()
        tcp_client_socket.send(senddata)
        num += 1

    tcp_client_socket.close()

    print("Send : %d" % num)



file_type = file_extension(target_file) 

if file_type == ".xlsx" :
    Xlsx_file_hander()

elif file_type == ".csv" :
    Csv_file_hander()
else :
    print("Unsupport file type : %s" % file_type)



