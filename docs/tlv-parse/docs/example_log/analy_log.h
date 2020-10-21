#ifndef __ANALY_LOG_H__
#define __ANALY_LOG_H__

#define field_helper_line(output_type,uname,log_id) \
{output_type,uname,log_id},

enum X10_LOG_TYPE{
	BASIC_FLOW_LOG = 0,
	HIT_RUL_LOG    = 0X01,
	BEAR_PROTO_LOG = 0X10,
	APP_PROTO_LOG  = 0X11,
};

enum XDR_FIELD_OUT_TYPE
{
    P_RT_U1 = 1 ,
    P_RT_U2     ,
    P_RT_U4     ,
    P_RT_U8     ,
    P_RT_ST     ,
    P_RT_IPV4   ,
    P_RT_IPV6   ,
    P_RT_STR_NODE ,
    P_RT_STR_LEN  ,
    P_RT_ST_L ,
    P_RT_NO_USE,
    P_RT_S2  ,
    P_RT_U2_HEX,
    P_RT_U8_ATM,
    P_RT_U8_DTM,
    P_RT_U2_S5 ,
    P_RT_U2_ST ,
    MAX_PRINT_TYPE_NUM,
};


typedef struct st_pcap_head
{
     unsigned int   magic;      ///<用来识别文件自己和字节顺序。0xa1b2c3d4用来表示按照原来的顺序读取，0xd4c3b2a1表示下面的字节都要交换顺序读取。一般使用0xa1b2c3d4
     unsigned short major;      ///<当前文件主要的版本号
     unsigned short minor;      ///<前文件次要的版本号
     unsigned int   thiszone;   ///<时区。GMT和本地时间的相差，用秒来表示。如果本地的时区是GMT，那么这个值就设置为0.这个值一般也设置为0
     unsigned int   sigfigs;    ///<时间戳的精度；全零
     unsigned int   snaplen;    ///<最大的存储长度（该值设置所抓获的数据包的最大长度，如果所有数据包都要抓获，将该值设置为65535
     unsigned int   linktype;   ///<链路类型
} t_pcap_head;


///pcap数据报头
typedef struct st_pkt_header
{
    unsigned int    ts_sec;     ///<被捕获时间的高位，单位是seconds
    unsigned int    ts_usec;    ///<被捕获时间的低位，单位是microseconds
    unsigned int    len;        ///<当前数据区的长度，即抓取到的数据帧长度，不包括Packet Header本身的长度，单位是 Byte ，由此可以得到下一个数据帧的位置。
    unsigned int    reallen;    ///<离线数据长度：网络中实际数据帧的长度，一般不大于caplen，多数情况下和Caplen数值相等。
    char            data[0];    ///<数据包的数据
} t_pkt_head;


typedef struct st_field_describe
{
    unsigned short   field_print_id;//字段对应的输出类型
    char             uname[64];     //字段名
    unsigned short   tag;
}field_describe;

#pragma pack(1)

typedef struct st_tlv_data
{
    unsigned short type;
    unsigned short len;
    char   value[0];
}t_tlv_data;

typedef struct st_x10_log_head
{
	struct{
		unsigned char major_ver;
		unsigned char minor_ver;
		unsigned short length;
	}header;
    unsigned char      msg_type;// 0 基本流日志 1 命中日志 2.承载日志 3 应用类别日志
    unsigned char      proto_type;//承载日志类型区分字段
    char        tlvs[0];
}t_x10_log_head;

#pragma pack()



#endif
