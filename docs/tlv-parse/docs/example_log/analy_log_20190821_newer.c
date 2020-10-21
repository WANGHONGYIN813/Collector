#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <netinet/in.h>

#include "analy_log.h"


#define DS_HEADER_FIELD \
          field_helper_line(P_RT_ST_L     ,"xDR ID"              ,1)\
          field_helper_line(P_RT_U8_ATM   ,"Procedure Start Time",2)\
          field_helper_line(P_RT_U8_DTM   ,"Flow Continue Time"  ,3)\
          field_helper_line(P_RT_U8_ATM   ,"Procedure End Time"  ,4)\
          field_helper_line(P_RT_ST       ,"Flow end reason"     ,5)

#define DS_COM_FIELD \
          field_helper_line(P_RT_IPV4,"USER_IPv4",100)\
          field_helper_line(P_RT_IPV6,"USER_IPv6",101)\
          field_helper_line(P_RT_U2_S5  ,"User Port",104)\
          field_helper_line(P_RT_U1  ,"L4 protocal",106)\
          field_helper_line(P_RT_IPV4,"App Server IP_IPv4",102)\
          field_helper_line(P_RT_IPV6,"App Server IP_IPv6",103)\
          field_helper_line(P_RT_U2_S5  ,"App Server Port",105)

#define DS_DNS_FIELD \
          field_helper_line(P_RT_ST_L     ,"xDR ID"              ,1)\
          field_helper_line(P_RT_U8_ATM   ,"Procedure Start Time",2)\
          field_helper_line(P_RT_ST, "Domain Name"   ,6)\
          field_helper_line(P_RT_U1, "IP Addr Num"   ,7)\
          field_helper_line(P_RT_U2, "IP Addr Length",8)\
          field_helper_line(P_RT_ST, "IP Addr"       ,9)\
          field_helper_line(P_RT_U1, "RCode"         ,10)\
          field_helper_line(P_RT_U1, "DNSReq Num"    ,11)\
          field_helper_line(P_RT_U1, "Ancount",12)\
          field_helper_line(P_RT_U1, "Nscount",13)\
          field_helper_line(P_RT_U1, "Arcount",14)\
          field_helper_line(P_RT_U4, "Response Time",15)\
          field_helper_line(P_RT_U2, "CName Length" ,16)\
          field_helper_line(P_RT_ST, "CName"        ,17)\
          field_helper_line(P_RT_U2, "QUERY_TYPE"   ,18)


#define DS_HTTP_FIELD \
          field_helper_line(P_RT_ST_L     ,"xDR ID"              ,1)\
          field_helper_line(P_RT_U8_ATM   ,"Procedure Start Time",2)\
          field_helper_line(P_RT_U1  ,"HTTP Version",6)\
          field_helper_line(P_RT_U2  ,"Message Type",7)\
          field_helper_line(P_RT_U2  ,"Message Status",8)\
          field_helper_line(P_RT_U4  ,"First HTTP Response Time",9)\
          field_helper_line(P_RT_U4  ,"Last Content Packet Time",10)\
          field_helper_line(P_RT_U4  ,"Last ACK Time",11)\
          field_helper_line(P_RT_STR_LEN  ,"HOST Length",12)\
          field_helper_line(P_RT_STR_NODE ,"HOST",13)\
          field_helper_line(P_RT_STR_LEN  ,"URI Length",14)\
          field_helper_line(P_RT_STR_NODE ,"URI",15)\
          field_helper_line(P_RT_ST       ,"HTTP Request type",16)\
          field_helper_line(P_RT_STR_LEN  ,"X-Online-Host Length",17)\
          field_helper_line(P_RT_STR_NODE ,"X-Online-Host",18)\
          field_helper_line(P_RT_STR_LEN  ,"User-Agent Length",19)\
          field_helper_line(P_RT_STR_NODE ,"User-Agent",20)\
          field_helper_line(P_RT_STR_NODE ,"HTTP_content_type",21)\
          field_helper_line(P_RT_STR_LEN  ,"refer_URI Length",22)\
          field_helper_line(P_RT_STR_NODE ,"refer_URI",23)\
          field_helper_line(P_RT_STR_LEN  ,"Cookie Length",24)\
          field_helper_line(P_RT_STR_NODE ,"Cookie",25)\
          field_helper_line(P_RT_U4       ,"Content-Length",26)\
          field_helper_line(P_RT_ST       ,"key word",27)\
          field_helper_line(P_RT_U1       ,"Service Behavior Flag",28)\
          field_helper_line(P_RT_U1       ,"Service Comp Flag",29)\
          field_helper_line(P_RT_U4       ,"Service Time",30)\
          field_helper_line(P_RT_U1       ,"IE",31)\
          field_helper_line(P_RT_U1       ,"Portal",32)\
          field_helper_line(P_RT_STR_LEN  ,"location Length",33)\
          field_helper_line(P_RT_STR_NODE ,"location",34)\
          field_helper_line(P_RT_U1       ,"first request",35)\
          field_helper_line(P_RT_ST       ,"User account",36)\
          field_helper_line(P_RT_U2       ,"URI type",37)\
          field_helper_line(P_RT_U4       ,"URI sub-type",38)


#define DS_FTP_FIELD \
          field_helper_line(P_RT_ST_L     ,"xDR ID"              ,1)\
          field_helper_line(P_RT_U8_ATM   ,"Procedure Start Time",2)\
          field_helper_line(P_RT_U2 ,"FTPStatus",6)\
          field_helper_line(P_RT_ST ,"User Name",7)\
          field_helper_line(P_RT_ST ,"File Path",8)\
          field_helper_line(P_RT_U1 ,"Trans Mode",9)\
          field_helper_line(P_RT_U1 ,"Trans Type",10)\
          field_helper_line(P_RT_ST ,"File Name",11)\
          field_helper_line(P_RT_U2 ,"Client Port",12)\
          field_helper_line(P_RT_U2 ,"Server Port",13)\
          field_helper_line(P_RT_U4 ,"File Size",14)\
          field_helper_line(P_RT_U4 ,"Response Time",15)\
          field_helper_line(P_RT_U4 ,"Trans Time",16)\
          field_helper_line(P_RT_ST ,"Request Command",17)\
          field_helper_line(P_RT_ST ,"Request Param",18)\
          field_helper_line(P_RT_U2 ,"Response Code",19)\
          field_helper_line(P_RT_ST ,"Response Param",20)

#define DS_EMAIL_FIELD \
          field_helper_line(P_RT_ST_L     ,"xDR ID"              ,1)\
          field_helper_line(P_RT_U8_ATM   ,"Procedure Start Time",2)\
          field_helper_line(P_RT_U2_HEX,"Message Type",3)\
          field_helper_line(P_RT_S2    ,"Status Type",4)\
          field_helper_line(P_RT_ST    ,"User Name",5)\
          field_helper_line(P_RT_ST    ,"Sender Info",6)\
          field_helper_line(P_RT_U4    ,"Email Length",7)\
          field_helper_line(P_RT_ST    ,"SMTP Domain Name",8)\
          field_helper_line(P_RT_ST    ,"Email Sender",9)\
          field_helper_line(P_RT_ST    ,"Email Receiver",10)\
          field_helper_line(P_RT_ST    ,"Email CCer",11)\
          field_helper_line(P_RT_ST    ,"Email Theme",12)\
          field_helper_line(P_RT_ST    ,"Receiver Account",13)\
          field_helper_line(P_RT_ST    ,"Mail Head Info",14)\
          field_helper_line(P_RT_U1    ,"Access Type",15)


#define DS_SIP_FIELD \
          field_helper_line(P_RT_ST_L     ,"xDR ID"              ,1)\
          field_helper_line(P_RT_U8_ATM   ,"Procedure Start Time",2)\
          field_helper_line(P_RT_U1,"Direction",3)\
          field_helper_line(P_RT_ST,"CallingNumber",4)\
          field_helper_line(P_RT_ST,"CalledNumber",5)\
          field_helper_line(P_RT_U1,"Type",6)\
          field_helper_line(P_RT_U1,"Reason",7)\
          field_helper_line(P_RT_U2,"Message Type",8)\
          field_helper_line(P_RT_U2,"Message Status",9)


#define DS_RTSP_FIELD \
          field_helper_line(P_RT_ST_L     ,"xDR ID"              ,1)\
          field_helper_line(P_RT_U8_ATM   ,"Procedure Start Time",2)\
          field_helper_line(P_RT_ST,"URL",3)\
          field_helper_line(P_RT_ST,"User-Agent",4)\
          field_helper_line(P_RT_ST,"RTP-Server-IP",5)\
          field_helper_line(P_RT_U2,"Client-Starting-Port",6)\
          field_helper_line(P_RT_U2,"Client-Ending-Port",7)\
          field_helper_line(P_RT_U2,"Server-Starting-Port",8)\
          field_helper_line(P_RT_U2,"Server-Ending-Port",9)\
          field_helper_line(P_RT_U2,"Video-Flow-Number",10)\
          field_helper_line(P_RT_U2,"Audio-Flow-Number",11)\
          field_helper_line(P_RT_U4,"Response-Delay",12)


#define DS_RADIUS_FIELD \
          field_helper_line(P_RT_ST_L     ,"xDR ID"              ,1)\
          field_helper_line(P_RT_U8_ATM   ,"Procedure Start Time",2)\
          field_helper_line(P_RT_ST,"startTime",3)\
          field_helper_line(P_RT_ST,"updateTime",4)\
          field_helper_line(P_RT_ST,"endTime",5)\
          field_helper_line(P_RT_ST,"Account",6)\
          field_helper_line(P_RT_ST,"userIP",7)\
          field_helper_line(P_RT_ST,"BASIP",8)\
          field_helper_line(P_RT_ST,"physical port",9)\
          field_helper_line(P_RT_ST,"logical port",10)\
          field_helper_line(P_RT_U4,"InBytes",11)\
          field_helper_line(P_RT_U4,"OutBytes",12)\
          field_helper_line(P_RT_ST,"NAT IP",13)\
          field_helper_line(P_RT_U2,"NAT port_start",14)\
          field_helper_line(P_RT_U2,"NAT port_end",15)\
          field_helper_line(P_RT_ST,"NAS-Identifier",16)

#define DS_HTTPS_FIELD \
         field_helper_line(P_RT_ST_L     ,"xDR ID"              ,1)\
         field_helper_line(P_RT_U8_ATM   ,"Procedure Start Time",2)\
         field_helper_line(P_RT_U4 ,"First HTTPS Response Time",6)\
         field_helper_line(P_RT_U4 ,"First Application data time",7)\
         field_helper_line(P_RT_U4 ,"Last Content Packet Time",8)\
         field_helper_line(P_RT_U4 ,"Last ACK Time",9)\
         field_helper_line(P_RT_STR_LEN  ,"SNI Length",10)\
         field_helper_line(P_RT_STR_NODE ,"SNI",11)\
         field_helper_line(P_RT_STR_LEN  ,"CN Length",12)\
         field_helper_line(P_RT_STR_NODE ,"CN",13)\
         field_helper_line(P_RT_ST  ,"HTTPS_CERTIFI_HOST_NAME",14)\
         field_helper_line(P_RT_ST  ,"HTTPS_ENCODE_TYPE",15)\
         field_helper_line(P_RT_ST  ,"HTTPS_CERTIFI_USER",16)\
         field_helper_line(P_RT_ST  ,"HTTPS_CERTIFI_AWARD_SERVER",17)\
         field_helper_line(P_RT_ST  ,"HTTPS_CERTIFI_AWARD_ORG",18)\
         field_helper_line(P_RT_ST  ,"HTTPS_CERTIFI_START_TIME",19)\
         field_helper_line(P_RT_ST  ,"HTTPS_CERTIFI_END_TIME",20)\
         field_helper_line(P_RT_ST  ,"HTTPS_STANDBY_HOST",21)


#define DS_X10_FIELD \
     field_helper_line(P_RT_ST ,"link operator_name",30)\
     field_helper_line(P_RT_ST ,"link net_short_name",31)\
     field_helper_line(P_RT_ST ,"link collect_place",32)\
     field_helper_line(P_RT_ST ,"link dev_nu",33)\
     field_helper_line(P_RT_ST ,"link equ_type",34)\
     field_helper_line(P_RT_ST ,"link equ_slot",35)\
     field_helper_line(P_RT_U1 ,"link link_type",36)\
     field_helper_line(P_RT_ST ,"link inface_speed_type",37)\
     field_helper_line(P_RT_U2 ,"link input_type",38)\
     field_helper_line(P_RT_U1 ,"link link_no",39)\
     field_helper_line(P_RT_U1 ,"link link_direc",40)\
     field_helper_line(P_RT_ST ,"ETH src_mac",60)\
     field_helper_line(P_RT_ST ,"ETH dst_mac",61)\
     field_helper_line(P_RT_ST_L ,"ETH out_vlan",62)\
     field_helper_line(P_RT_ST_L ,"ETH inner_vlan",63)\
     field_helper_line(P_RT_ST_L ,"IP ip_ver",80)\
     field_helper_line(P_RT_U1 ,"IP ip_dscp",81)\
     field_helper_line(P_RT_U2 ,"IP ip_len",82)\
     field_helper_line(P_RT_U2 ,"IP frag_id",83)\
     field_helper_line(P_RT_U1 ,"IP more_frag_flag",84)\
     field_helper_line(P_RT_U1 ,"IP enable_frag",85)\
     field_helper_line(P_RT_U2 ,"IP first_offset",86)\
     field_helper_line(P_RT_U1 ,"IP ip_ttl",87)\
     field_helper_line(P_RT_ST ,"IP ipv6_ext",88)\
     field_helper_line(P_RT_U1 ,"tunnel_num_1",130)\
     field_helper_line(P_RT_ST ,"tunnel_type_1",131)\
     field_helper_line(P_RT_ST_L ,"tunnel_ip_type_1",132)\
     field_helper_line(P_RT_IPV4 ,"tunnel_sip_1",133)\
     field_helper_line(P_RT_IPV6 ,"tunnel_src128_1",134)\
     field_helper_line(P_RT_ST   ,"tunnel_head_info_1",135)\
     field_helper_line(P_RT_IPV4 ,"tunnel_dest_1",136)\
     field_helper_line(P_RT_IPV6 ,"tunnel_dst128_1",137)\
     field_helper_line(P_RT_U2   ,"tunnel_sport_1",138)\
     field_helper_line(P_RT_U2   ,"tunnel_dport_1",139)\
     field_helper_line(P_RT_U1   ,"tunnel_L4_ptoto_1",140)\
     field_helper_line(P_RT_U1   ,"tunnel_num_2",160)\
     field_helper_line(P_RT_ST   ,"tunnel_type_2",161)\
     field_helper_line(P_RT_ST_L  ,"tunnel_ip_type_2",162)\
     field_helper_line(P_RT_IPV4 ,"tunnel_sip_2",163)\
     field_helper_line(P_RT_IPV6 ,"tunnel_src128_2",164)\
     field_helper_line(P_RT_ST   ,"tunnel_head_info_2",165)\
     field_helper_line(P_RT_IPV4 ,"tunnel_dest_2",166)\
     field_helper_line(P_RT_IPV6 ,"tunnel_dst128_2",167)\
     field_helper_line(P_RT_U2   ,"tunnel_sport_2",168)\
     field_helper_line(P_RT_U2   ,"tunnel_dport_2",169)\
     field_helper_line(P_RT_U1   ,"tunnel_L4_ptoto_2",170)\
     field_helper_line(P_RT_U4   ,"HASH_FLOW ipv4_hash",180)\
     field_helper_line(P_RT_U4   ,"HASH_FLOW ipv6_hash",181)\
     field_helper_line(P_RT_U2   ,"HASH_FLOW acl_rul_id",182)\
     field_helper_line(P_RT_U1   ,"HASH_FLOW out_port_group_id",183)\
     field_helper_line(P_RT_ST   ,"HASH_FLOW buss_id",184)\
     field_helper_line(P_RT_U4   ,"TCP seq",200)\
     field_helper_line(P_RT_U4   ,"TCP next_seq",201)\
     field_helper_line(P_RT_U1   ,"TCP tcp_head_len",202)\
     field_helper_line(P_RT_U1   ,"TCP tcp_res_flag",203)\
     field_helper_line(P_RT_U1   ,"TCP tcp_ctl_flag",204)\
     field_helper_line(P_RT_U2   ,"Window",205)\
     field_helper_line(P_RT_U1   ,"TCP tcp_check_sum",206)\
     field_helper_line(P_RT_U2   ,"TCP urg_flag",207)\
     field_helper_line(P_RT_U2_ST  ,"UL Disorder IP Packet",208)\
     field_helper_line(P_RT_U2_ST  ,"DL Disorder IP Packet",209)\
     field_helper_line(P_RT_U2_ST  ,"UL Retrans IP Packet",210)\
     field_helper_line(P_RT_U2_ST  ,"DL Retrans IP Packet",211)\
     field_helper_line(P_RT_U8_DTM ,"TCP Response Time",212)


#define DS_HIT_CONFLICT_FIELD \
     field_helper_line(P_RT_ST   ,"HIT buss_id",250)\
     field_helper_line(P_RT_ST   ,"HIT rul_type",251)\
     field_helper_line(P_RT_U2   ,"HIT rul_group_id",252)\
     field_helper_line(P_RT_ST   ,"HIT pcap_index",253)\
     field_helper_line(P_RT_ST_L ,"HIT direct",254)\
     field_helper_line(P_RT_ST   ,"HIT hit_num",255)\
     field_helper_line(P_RT_U1   ,"HIT out_port_group_id",256)\
     field_helper_line(P_RT_STR_NODE ,"HIT HOST",300)\
     field_helper_line(P_RT_STR_NODE ,"HIT URI",301)\
     field_helper_line(P_RT_STR_NODE ,"HIT User-Agent",302)\
     field_helper_line(P_RT_ST       ,"HIT HTTP Request type",303)\
     field_helper_line(P_RT_STR_NODE ,"HIT SNI",350)\
     field_helper_line(P_RT_ST   ,"HIT dns Domain Name",380)\
     field_helper_line(P_RT_U2   ,"HIT dns QUERY_TYPE",381)\
     field_helper_line(P_RT_ST   ,"HIT dns IP Addr",382)\
     field_helper_line(P_RT_ST   ,"HIT dns match method",383)\
     field_helper_line(P_RT_ST   ,"HIT Email Sender",400)\
     field_helper_line(P_RT_ST   ,"HIT Email Receiver",401)\
     field_helper_line(P_RT_ST   ,"HIT Email CCer",402)\
     field_helper_line(P_RT_ST   ,"HIT Email Theme",403)



#define DS_BASIC_CONFLICT_FIELD \
    field_helper_line(P_RT_ST   ,"ACCESS access_type",250)\
    field_helper_line(P_RT_ST   ,"ACCESS access_attribute",251)\
    field_helper_line(P_RT_ST   ,"ACCESS country",252)\
    field_helper_line(P_RT_ST   ,"ACCESS province",253)\
    field_helper_line(P_RT_ST   ,"ACCESS city",254)\
    field_helper_line(P_RT_ST   ,"ACCESS county",255)\
    field_helper_line(P_RT_ST   ,"ACCESS town",256)\
    field_helper_line(P_RT_ST   ,"ACCESS street",257)\
    field_helper_line(P_RT_ST   ,"ACCESS s_GPS_longitude",258)\
    field_helper_line(P_RT_ST   ,"ACCESS c_GPS_longitude",259)\
    field_helper_line(P_RT_ST   ,"ACCESS s_GPS_latitude",260)\
    field_helper_line(P_RT_ST   ,"ACCESS c_GPS_latitude",261)\
    field_helper_line(P_RT_ST   ,"ACCESS s_GPS_height",262)\
    field_helper_line(P_RT_ST   ,"ACCESS c_GPS_height",263)\
    field_helper_line(P_RT_ST   ,"ACCESS coordinate",264)\
    field_helper_line(P_RT_ST_L ,"STAT unknowe_flag",300)\
    field_helper_line(P_RT_U8   ,"UL IP Packet",301)\
    field_helper_line(P_RT_U8   ,"UL Data",302)\
    field_helper_line(P_RT_U8   ,"DL IP Packet",303)\
    field_helper_line(P_RT_U8   ,"DL Data",304)\
    field_helper_line(P_RT_U2   ,"STAT up_bps",305)\
    field_helper_line(P_RT_U2   ,"STAT dn_bps",306)\
    field_helper_line(P_RT_U2   ,"STAT up_pps",307)\
    field_helper_line(P_RT_U2   ,"STAT dn_pps",308)\
    field_helper_line(P_RT_U2   ,"RELATION hit_rul_log_id",390)\
    field_helper_line(P_RT_U2   ,"RELATION bear_proto_log_id",391)\
    field_helper_line(P_RT_U2   ,"RELATION app_proto_log_id",392)

#define DS_PROTO_CONFLICT_FIELD \
     field_helper_line(P_RT_ST  ,"IDFY app_type_name",50)\
     field_helper_line(P_RT_ST  ,"IDFY app_type code",51)\
     field_helper_line(P_RT_ST  ,"IDFY APP name",52)\
     field_helper_line(P_RT_ST  ,"IDFY APP code",53)\
     field_helper_line(P_RT_ST  ,"IDFY app_proto ID",54)\
     field_helper_line(P_RT_ST  ,"IDFY app_uni_proro code",55)


 field_describe g_field_describe[14][50] = {
     {{0,"null",0}},
     {DS_DNS_FIELD},
     {{0,"null",0}},
     {DS_HTTP_FIELD},
     {DS_FTP_FIELD},
     {DS_EMAIL_FIELD},
     {DS_SIP_FIELD},
     {DS_RTSP_FIELD},
     {DS_RADIUS_FIELD},
     {{0,"null",0}},
     {{0,"null",0}},
     {{0,"null",0}},
     {{0,"null",0}},
     {DS_HTTPS_FIELD},
 };


field_describe g_basic_field_describe[] = {
    DS_HEADER_FIELD
    DS_COM_FIELD
    DS_BASIC_CONFLICT_FIELD
	DS_X10_FIELD
};

field_describe g_hit_field_describe[] = {
    DS_HEADER_FIELD
    DS_COM_FIELD
    DS_HIT_CONFLICT_FIELD
	DS_X10_FIELD
};

field_describe g_proto_field_describe[] = {
    DS_HEADER_FIELD
    DS_PROTO_CONFLICT_FIELD
};



static long jump_pcap_hdr(FILE* stream)
{
    fseek(stream,sizeof(t_pcap_head),SEEK_SET);
    return sizeof(t_pcap_head);
}

static unsigned int analy_pkt_hdr(FILE* stream)
{
    char buf[32] = {0};
    fread(buf, sizeof(t_pkt_head), 1, stream);
    t_pkt_head *pkt_hdr = (t_pkt_head *)buf;
    return pkt_hdr->len;
}

static long filesize(FILE *stream)
{
   long curpos, length;

   curpos = ftell(stream);
   fseek(stream, 0L, SEEK_END);
   length = ftell(stream);
   fseek(stream, curpos, SEEK_SET);
   return length;
}

static field_describe* get_field_describe(unsigned char pro_type,unsigned char log_type,int *num)
{
    if(BASIC_FLOW_LOG == log_type)
    {
        *num = sizeof(g_basic_field_describe)/sizeof(field_describe);
        return g_basic_field_describe;
    }
    else if(HIT_RUL_LOG == log_type)
    {
        *num = sizeof(g_hit_field_describe)/sizeof(field_describe);
        return g_hit_field_describe;
    }
    else if(BEAR_PROTO_LOG == log_type)
    {
        *num = 50;
        return g_field_describe[pro_type];
    }
    else if(APP_PROTO_LOG == log_type)
    {
        *num = sizeof(g_proto_field_describe)/sizeof(field_describe);
        return g_proto_field_describe;
    }
    else
    {
        return NULL;
    }
}


static void write_2file(FILE *fw,unsigned short data_type,unsigned short data_len,
                        char *data,unsigned char pro_type,unsigned char log_type)
{
    int num = 0;
    field_describe *field = get_field_describe(pro_type,log_type,&num);

    int i = 0;
    for(i=0 ; i< num; i++)
	{
	    field_describe *p_field = field+i;
		if(p_field->tag  == data_type)
		{
		    //u1
			if(P_RT_U1 == p_field->field_print_id)
			{
				fprintf(fw,"\"%s\" = %hhu,",p_field->uname,*(unsigned char *)(data));
				break;
			}
			//u2
			else if((P_RT_U2 == p_field->field_print_id) ||\
				(P_RT_STR_LEN == p_field->field_print_id) ||\
				(P_RT_S2 == p_field->field_print_id)||\
				(P_RT_U2_HEX == p_field->field_print_id))
			{
				fprintf(fw,"\"%s\" = %hu,",p_field->uname,ntohs(*(unsigned short *)(data)));
				break;
			}
			//u4
			else if((P_RT_U4 == p_field->field_print_id))
			{
				fprintf(fw,"\"%s\" = %u,",p_field->uname,ntohl(*(unsigned int *)(data)));
				break;
			}
			//u8
			else if((P_RT_U8 == p_field->field_print_id))
			{
				fprintf(fw,"\"%s\" = %lu,",p_field->uname,ntohl(*(unsigned long *)(data)));
				break;
			}
			//str
			else
			{
				fprintf(fw,"\"%s\" = \"%.*s\",",p_field->uname,data_len,data);
				break;
			}
		}
	}
}


static void write_tlv_info(FILE *fw,char *tlv_data,unsigned short tlv_data_len,
                            unsigned char pro_type,unsigned char log_type)
{
    char *p_tlv = tlv_data;
    unsigned short left_len = tlv_data_len;
    if(left_len <= sizeof(t_tlv_data))
    {
        return;
    }

    t_tlv_data *tlv = NULL;
    unsigned short data_type = 0;
    unsigned short data_len  = 0;
    while(left_len > sizeof(t_tlv_data))
    {
        tlv = (t_tlv_data*)p_tlv;
        data_type = ntohs(tlv->type);
        data_len  = ntohs(tlv->len);

        if(data_type > 403)
        {
            fprintf(stderr, "tlv payload is err!\n");
            break;//数据部分有问题，不继续解析
        }
        else if(data_len >= left_len)
        {
            fprintf(stderr, "tlv payload is err!\n");
            break;//数据部分有问题，不继续解析
        }

        write_2file(fw,data_type,data_len,tlv->value,pro_type,log_type);

        p_tlv    += (sizeof(t_tlv_data)+data_len);
        left_len -= (sizeof(t_tlv_data)+data_len);
    }

    //最后会有一个逗号，没关系
    fprintf(fw,"\n");
}




static void get_log_info(FILE *fw,char *payload,unsigned int payload_len)
{
    t_x10_log_head *log_hdr = (t_x10_log_head*)payload;
    unsigned short  log_len = ntohs(log_hdr->header.length);
    if(log_len != payload_len)
    {
        fprintf(stderr, "the log_pcap payload_len is [%u],it payload_len is err!\n",payload_len);
        return;
    }
    else if((BASIC_FLOW_LOG != log_hdr->msg_type)&&\
            (HIT_RUL_LOG    != log_hdr->msg_type)&&\
            (BEAR_PROTO_LOG != log_hdr->msg_type)&&\
            (APP_PROTO_LOG  != log_hdr->msg_type))
    {
        fprintf(stderr, "the log_pcap payload_len is [%u],it log_type is err!\n",payload_len);
        return;
    }

    fprintf(fw,"log_type = %02x,log_len = %hu,proto_type = %hhu,",log_hdr->msg_type,log_len,log_hdr->proto_type);

    //写入tlv字段信息
    char *tlv_data = payload+sizeof(t_x10_log_head);
    unsigned short tlv_data_len = log_len-sizeof(t_x10_log_head);
    unsigned char  proto_type = 0;
    if(log_hdr->proto_type > 100)
    {
        proto_type = log_hdr->proto_type - 100;
    }
    write_tlv_info(fw,tlv_data,tlv_data_len,proto_type,log_hdr->msg_type);
}

static FILE* make_log_file(void)
{
	struct tm cur_tm = {0};
	time_t time_sec = time(NULL);

	localtime_r(&time_sec,&cur_tm);
	char time_str[32] = {0};
	strftime(time_str, 32, "%Y%m%d%H%M%S", &cur_tm);

    char file_name[64] = {0};
    snprintf(file_name,64,"%s.txt",time_str);

	FILE *fw = fopen(file_name,"a+");
	if(fw == NULL)
	{
		return NULL;
	}
    return fw;
}


static void analy_payload_log(FILE* stream,FILE *fw,unsigned int pkt_len)
{
    char *pkt_buf = (char*)malloc(sizeof(char)*pkt_len);
    if(NULL == pkt_buf)
    {
        return;
    }

    //解析
    if (fread(pkt_buf, pkt_len, 1, stream) > 0)
    {
        char *payload = pkt_buf+14+4+20+8;
        unsigned int payload_len = pkt_len-(14+4+20+8);
        //写日志信息
        get_log_info(fw,payload,payload_len);
    }
    //
    free(pkt_buf);
    pkt_buf = NULL;
    return;
}

static void analy_log_pcap(FILE* stream)
{
    //打开一个日志文件
    FILE *fw = make_log_file();
    if(NULL == fw)
    {
        return;
    }

    long pcap_len = filesize(stream);
    if(pcap_len < (sizeof(t_pcap_head)))
    {
        return;
    }
    long cur_pos  = jump_pcap_hdr(stream);
    long pkts_len = pcap_len-cur_pos;

    while(pkts_len > sizeof(t_pkt_head))
    {
        //读取一个包长度
        unsigned int one_pkt_len = analy_pkt_hdr(stream);
        //printf("pcap_len = %u\n",one_pkt_len);

        //解析payload日志
        analy_payload_log(stream,fw,one_pkt_len);

        //这两行其实可以不需要，读取的时候会偏移掉
        //cur_pos += (sizeof(t_pkt_head)+one_pkt_len);
        //fseek(stream, cur_pos, SEEK_SET);

        pkts_len -= (sizeof(t_pkt_head)+one_pkt_len);
    }

    fclose(fw);
}

static FILE* open_log_pcap(char *pcap_file)
{
    FILE* fd = fopen(pcap_file,"r");
	if(!fd)
	{
		printf("read log_pcap file err!\n");
		return NULL;
	}
	return fd;
}

int main(int argc, char **argv)
{
    if(argc != 2)
    {
        printf("pleasr input :like ./exe xxx.pcap\n");
        return 0;
    }
    FILE* fd = open_log_pcap(argv[1]);
    if(fd)
    {
        analy_log_pcap(fd);
        fclose(fd);
    }
    return 1;
}

