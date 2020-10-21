

import socket


ip = '0.0.0.0'
port = 8000
filepath = ""

import sys
import getopt


def help():
    print("-i 0.0.0.0 -p 8000 -f 12.bin\n")



def OptParse(argv):
    global ip
    global port
    global filepath
 
    try:
        opts, args = getopt.getopt(argv, "i:p:f:")
    except getopt.GetoptError:
        help()
        sys.exit()
    for opt, arg in opts:
        if opt in ("-i"):
            ip = arg
        elif opt in ("-p"):
            port = int(arg)
        elif opt in ("-f"):
            filepath = arg 
        else:
            help()
            sys.exit()
 

if __name__ == "__main__":

    OptParse(sys.argv[1:])

    if ip == '':
        help()
        sys.exit()

    f = open(filepath,"rb")
    fr = f.read()

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    s.sendto(fr, (ip, port))

    s.close()


