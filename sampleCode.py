from pyspark import SparkContext
sc = SparkContext()
from pyspark.sql import HiveContext
spark = HiveContext(sc)
sqlContext = spark

from pyspark.sql import functions as F
from pyspark.sql import Window 
from pyspark.sql.functions import lit
from pyspark.sql import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
import sys
import time
from datetime import datetime, timedelta as td
from pyspark.sql.types import *
import re,requests,json
from pyspark.sql.functions import col, when, max, min
from pyspark.sql.functions import count, sum,avg
from pyspark.sql.functions import udf
from functools import reduce
from pyspark.sql.types import StringType
from pyspark.sql.functions import lit
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import broadcast

import itertools
import re
import os

from datetime import datetime
import requests, json, pytz

JOB_ID = 2

logging_mapping=spark.read.parquet("s3://travcompany-term/project_traveler_profile/testSSS/tmp/logging_table")

filter_logging = logging_mapping.filter((col("job_id") == JOB_ID)).head()


log_job_type =  filter_logging["job_type"]
log_job_name = filter_logging["job_name"]

log_scheduled_time = filter_logging['scheduled_date'] + " " + filter_logging["scheduled_start_time"]
log_spected_end_time = filter_logging['scheduled_date'] + " " + filter_logging["scheduled_end_time"]
log_spected_duration = filter_logging['spected_duration']


log_actual_time = datetime.now(pytz.timezone('US/Pacific'))
log_schedule_date = filter_logging['scheduled_date']

is_complete = 0


try:
   
    def parse_start_date(evar30, product_list):
        if evar30:
            date_details = evar30
        else:
            date_details = product_list

        _start_date_group = re.findall(re.compile(r':(\s*\d{8}[^:\-]*)([-]?)'), date_details)
        _start_date = []

        if _start_date_group:
            for s in _start_date_group:
                try:
                    s = s[0].strip()
                    s = datetime.strptime(s, '%Y%m%d').strftime('%Y-%m-%d')
                    _start_date.append(s)
                except:
                    pass

        inbound_date = sorted(_start_date)[0] if _start_date else None
        return inbound_date
    parse_start_date_UDF = udf(parse_start_date, StringType())


    def parse_end_date(evar30, product_list):
        if evar30:
            date_details = evar30
        else:
            date_details = product_list
        
        _end_date_group = re.findall(re.compile(r'-([^:\-|]*)'), date_details)
        _end_date = []
        
        if _end_date_group:
            for e in _end_date_group:
                try:
                    e = e.strip()
                    e = datetime.strptime(e, '%Y%m%d').strftime('%Y-%m-%d')
                    _end_date.append(e)
                except:
                    pass

        end_date = sorted(_end_date, reverse=True)[0] if _end_date else None
        return end_date
    parse_end_date_UDF = udf(parse_end_date, StringType())


    def evar30_array(evar30):
        evar_arr = evar30.split("|")
        return evar_arr
    evar30_array_UDF = udf(evar30_array, ArrayType(StringType()))

    def product_list_array(product_list):
        product_list_arr = product_list.split(",;")
        return product_list_arr
    product_list_array_UDF = udf(product_list_array, ArrayType(StringType()))

    def parse_flight_type(search_parameter, lob):
        air_trip_type = None
        
        if "FLIGHT" in lob:
            if search_parameter is None:
                return None
            if "|OW|" in search_parameter:
                air_trip_type = "OW"
            if "|RT|" in search_parameter:
                air_trip_type = "RT"
            if "|MD|" in search_parameter:
                air_trip_type = "MD"
        return air_trip_type

    parse_flight_type_udf = udf(parse_flight_type, StringType())

    current_date_minus_2 = (datetime.today() - td(days=2)).date()
    current_date_minus_4 = (datetime.today() - td(days=4)).date()

    query_cancel = "select order_nbr, trvl_acct_tpid, trl from dm.traveler_profile_ctf where trans_typ = 'CANCEL' and trans_date > '" + str(current_date_minus_4) + "' and trans_date <= '" + str(current_date_minus_2) + "'"

    cancel_trans_fact = spark.sql(query_cancel)
    path = "s3://travcompany-term/project_traveler_profile/testSSS/cross_sell/cancellations/" + str(current_date_minus_2) + "/"

    cancel_trans_fact.write.mode("overwrite").parquet(path)


    # today's date
    date = (datetime.today() - td(days=2)).date()
    filter_date = date+td(2)

    prev_date = date - td(days=1)

    canc_date = date - td(days=0)

    ## These are just filler dates as they are used in the Omniture query.
    # Earlier use case is deprecated and moved to partitions captured on the current day.
    st = datetime.today()
    et = datetime.today()

    def extract(s):
        out_1 = re.search(r'\((.*?)\)', s)        
        if out_1 is None:
            return ''
        else:
            return out_1.group(1).split("-")[0]
    extractUDF = udf(extract, StringType())


    

    s_1 = spark.sql("select trim(hit_time_gmt) as hit_time_gmt,trim(accept_language) as accept_language,trim(date_time) as date_time,trim(visid_high) as visid_high, trim(visid_low) as visid_low,trim(page_event) as page_event,trim(pagename) as pagename,cast(trim(prop5) as Date) as check_in1,cast(trim(prop6) as Date) as check_out1,trim(prop11) as tuid,trim(prop12) as guid,trim(prop13) as spuser_id,trim(user_agent) as user_agent,trim(visit_num) as visit_num,trim(geo_city) as geo_city,trim(geo_country) as geo_country,trim(geo_region) as geo_region,trim(post_evar3) as origin,trim(post_evar4) as destination,cast(trim(post_evar5) as Integer) as search_window,cast(trim(post_evar6) as Integer) as trip_duration,trim(post_evar33) as marketing_code,trim(post_evar47) as search_parameters,trim(post_evar50) as device_info,trim(mobile_id) as mobile_id,cast(trim(local_dt) as Date) as local_dt ,trim(site_name) as site_name, trim(post_evar2) as LOB,product_list,post_product_list,channel,trim(post_evar30) as product_details,trim(prop71) as trl,trim(purchaseid) as purchaseid,trim(prop72) as OMS_Order_ID,trim(event_list) as event_list,file_name, trim(prop23) as app_guid, trim(evar13) as paid_omni from dm.omniture_hit_hourly where local_dt >='"+str(date)+"'")\
        .withColumn("gmt_dtm",(from_unixtime(col("hit_time_gmt")))).withColumn("gmt_dtm2",(from_unixtime(col("hit_time_gmt"))).cast("timestamp"))\
        .withColumn("start_time",(from_unixtime(unix_timestamp(lit(st), 'yyyy-MM-dd HH:mm:ss'))).cast("timestamp"))\
        .withColumn("end_time",(from_unixtime(unix_timestamp(lit(et), 'yyyy-MM-dd HH:mm:ss'))).cast("timestamp"))\
        .withColumn("tmp1",split(("file_name"),"_")[1])\
        .withColumn("tmp2",concat(substring(col("tmp1"),1,4),lit("-"),substring(col("tmp1"),5,2),lit("-"),substring(col("tmp1"),7,2),lit(" "),substring(col("tmp1"),10,2),lit(":"),substring(col("tmp1"),12,2),lit(":"),substring(col("tmp1"),14,2)))\
        .withColumn("upload_date",(from_unixtime(unix_timestamp(col("tmp2"), 'yyyy-MM-dd HH:mm:ss'))).cast("timestamp")).drop("tmp1").drop("tmp2")\
        .filter("upper(pagename) not like '%ERROR%' and upper(pagename) not like '%CHANGE%'")\
        .withColumn("guid_proper",regsp_replace(trim(upper(col("guid"))),'-',''))\
        .withColumn("app_guid_proper",regsp_replace(trim(upper(col("app_guid"))),'-',''))\
        .withColumn("final_guid_proper",F.when(col("guid_proper") == '00000000000000000000000000000000', col("app_guid_proper")).otherwise(col("guid_proper")))\
        .drop("guid").withColumnRenamed("final_guid_proper","guid")\
        .drop("app_guid").drop("app_guid_proper").drop("guid_proper")\
        .withColumn("paid_proper", split(split(upper(col("paid_omni")), "PAID")[1], "\.\s*")[0])\
        .withColumn("paid", trim(F.regsp_replace(col("paid_proper"),'[a-zA-Z]', '')))\
        .drop("paid_omni").drop("paid_proper")


    file_name_stored_data_yest = spark.read.parquet("s3://travcompany-term/project_traveler_profile/testSSS/tmp/rnz/file_name_stored_data/")
    broadcast(file_name_stored_data_yest)
    
    ### to keep the number of partitions processed consistent across days.
    partition_date = str(date + td(days=1)).replace('-','')
    max_allowable_partition = 'travcompanyglobal_' + partition_date + '-130000'
    
    ## consider only new partitions retrieved today and not processed in previous day's feed.
    s_raw = s_1.join(file_name_stored_data_yest, trim(s_1.file_name) == trim(file_name_stored_data_yest.file_name) , 'leftouter').select(s_1['*'], file_name_stored_data_yest.file_name.alias("file_name_yest")).filter("file_name_yest is null").drop("file_name_yest").filter(col("file_name") <= lit(max_allowable_partition))

    file_name_stored_data_yest.unpersist()

    s_raw.write.mode('overwrite').parquet("s3://travcompany-term/project_traveler_profile/testSSS/tmp/raw_omniture_data/" + str(date)[0:4] + "/" + str(date)[5:7]+ "/" + str(date)[8:10] + "/")


    s = spark.read.parquet("s3://travcompany-term/project_traveler_profile/testSSS/tmp/raw_omniture_data/" + str(date)[0:4] + "/" + str(date)[5:7]+ "/" + str(date)[8:10] + "/")


    non_packages = s.filter("LOB not in ('PACKAGE:FH','PACKAGE:FHC','PACKAGE:FC','PACKAGE:HC')")\
            .filter("event_list like '%,1,%'")\
            .withColumn("check_in2", parse_start_date_UDF(s.product_details, s.product_list))\
            .withColumn("check_out2", parse_end_date_UDF(s.product_details, s.product_list))\
            .withColumn("package_booking", lit(None))


    # Extract all package bookings and find check in and check out dates
    packages_temp = s.filter("LOB in ('PACKAGE:FH','PACKAGE:FHC','PACKAGE:FC','PACKAGE:HC')")\
        .filter("event_list like '%,1,%'")

    packages = packages_temp.filter("product_details like '%PACKAGE%'").select("*")\
        .withColumn("evar30_arr", evar30_array_UDF(packages_temp.product_details))

    packages_alternate = packages_temp.filter("product_details not like '%PACKAGE%'").select("*")\
        .withColumn("evar30_arr", product_list_array_UDF(packages_temp.product_list))


    sploded_packages_1 = packages\
        .select("*", splode("evar30_arr").alias("package_booking"))\
        .filter("package_booking not like '%PACKAGE:%'")

    sploded_packages_2 = packages_alternate\
        .select("*", splode("evar30_arr").alias("package_booking"))\
        .filter("package_booking like '%FLIGHT:%' or package_booking like '%HOTEL:%' or package_booking like '%CAR:%' or package_booking like '%LX:%' or package_booking like '%LX-GT:%'")

    sploded_packages = sploded_packages_1.unionAll(sploded_packages_2)

    package_bookings = sploded_packages.select("*")\
        .withColumn("check_in2", parse_start_date_UDF(sploded_packages.package_booking, sploded_packages.package_booking))\
        .withColumn("check_out2", parse_end_date_UDF(sploded_packages.package_booking, sploded_packages.package_booking))\
        .drop("evar30_arr")

    package_bookings = package_bookings.distinct()

    # Union package and non-package bookings 
    all_bookings = non_packages.unionAll(package_bookings.select(non_packages.columns))


    all_searches = s.filter("event_list not like '%,1,%'")\
        .withColumn("check_in2", lit(None))\
        .withColumn("check_out2",lit(None))\
        .withColumn("package_booking", lit(None))

    all_activities = all_bookings.unionAll(all_searches.select(all_bookings.columns))

        
    search = all_activities.withColumn("check_in3", coalesce("check_in2","check_in1")).withColumn("check_out3",coalesce("check_out2","check_out1")).withColumn("check_in_temp", F.when(col("check_in3").isNull(),col("check_out3")).otherwise(col("check_in3"))).withColumn("check_out_temp", F.when(col("check_out3").isNull(), col("check_in3")).otherwise(col("check_out3"))).withColumn("Activity_type", F.when((col("event_list").like('%,1,%')) & (col("purchaseid")!=''), "B").otherwise("S")).drop("check_in3").drop("check_out3").drop("check_in2").drop("check_out2").drop("check_in1").drop("check_out1")

    search = search.withColumn("check_in", col("check_in_temp").cast("Date")).withColumn("check_out", col("check_out_temp").cast("Date")).drop("check_out_temp").drop("check_in_temp")

    search = search.filter(F.col("check_in")<"2040-01-01").filter(F.col("check_out")<"2040-01-01").filter(F.col("check_in")>=date).filter(F.col("check_out")>=date).withColumnRenamed("spuser_id","spuserid").filter(col("check_out")>=col("check_in")).filter(col("check_in")>=col("local_dt"))

    map = spark.read.parquet("s3://travcompany-term/project_traveler_profile/testSSS/migration1/spid_guid_univ/").select( "guid", col("spuserid").alias("spuserid_map"))


    mapped_guid_not_paid = search.filter("spuserid is null or spuserid<=0 or spuserid=''").join(map, "guid", 'left_outer').select(search["*"],col("spuserid_map").alias("spuser_id")).drop("spuserid")

    unmapped_guid = search.filter("spuserid is not null and spuserid>0 and spuserid<>''").withColumnRenamed("spuserid","spuser_id")

    
    mapped_guid_paid = mapped_guid_not_paid.filter((col("paid").isNotNull()) & (col("paid") <> '') & (length(col("paid")) < 10) & (col("paid") <> '-1')).filter("spuser_id is null or spuser_id<=0 or spuser_id=''")\
        .withColumn("is_negative", F.when(col("paid") < 0 , 1).otherwise(lit(-1)))\
        .withColumn("spuserid", ((col("paid") * col("is_negative"))).cast(LongType())).drop("spuser_id")\
        .withColumn("spuser_id", col("spuserid").cast(StringType())).drop("spuserid")

    mapping_paid_negative_today = mapped_guid_paid.select("paid","spuser_id","is_negative").distinct()

    mapping_paid_negative_yesterday = spark.read.parquet("s3://travcompany-term/project_traveler_profile/testSSS/tmp/vb/mapping_negative_paid/" + str(prev_date)[0:4] +"/" + str(prev_date)[5:7]+"/"+str(prev_date)[8:10]+"/")

    mapping_paid_negative = mapping_paid_negative_today.unionAll(mapping_paid_negative_yesterday.select(mapping_paid_negative_today.columns)).distinct()

    mapping_paid_negative.write.mode("overwrite").parquet("s3://travcompany-term/project_traveler_profile/testSSS/tmp/vb/mapping_negative_paid/" + str(date)[0:4] +"/" + str(date)[5:7]+"/"+str(date)[8:10]+"/")

    unmapped_paid = mapped_guid_not_paid.filter("spuser_id is not null and spuser_id>0 and spuser_id<>''")

    final_mapped = unmapped_guid.unionAll(mapped_guid_paid.select(unmapped_guid.columns)).unionAll(unmapped_paid.select(unmapped_guid.columns))

    valid_spuser = spark.read.parquet("s3://travcompany-term/project_traveler_profile/testSSS/tmp/sk/mappings/gdpr_compliant_spuserids/").withColumnRenamed("spuserid","spuser_id")

    positive_spuser = final_mapped.filter("spuser_id > '0'")
    negative_spuser = final_mapped.filter("spuser_id < '0'")

    read_mapped_positive = positive_spuser.join(valid_spuser, "spuser_id", "inner")

    read_mapped = read_mapped_positive.unionAll(negative_spuser.select(read_mapped_positive.columns))

    read_mapped.cache()

    product_ln_name_creation = read_mapped.withColumn("product_list_lower", lower(col("product_list")))\
        .withColumn("product_ln_name",
            F.when((col("LOB").isin('HOTEL', 'HOTELS','HOTELS-TRAVELGUIDES')) | 
                (((col("product_list_lower").like('%hotel%')) | (col("product_list_lower").like('%3ph%'))) & (~col("product_list_lower").like('%package%')) & (~col("product_list_lower").like('%pkg%')) & (~col("product_list_lower").like('%rt%+%'))) |
                (col("package_booking").like('%HOTEL%')),'HOTEL')\
            .when((col("LOB").like('FLIGHT%')) | 
                (((col("product_list_lower").like('%flight%')) | (col("product_list_lower").like('%3pf%'))) & (~col("product_list_lower").like('%package%')) & (~col("product_list_lower").like('%pkg%')) & (~col("product_list_lower").like('%rt%+%'))) |
                (col("package_booking").like('%FLIGHT%')),'FLIGHT')\
            .when((col("LOB").like('CAR%')) | 
                ((col("product_list_lower").like('%car%')) & (~col("product_list_lower").like('%package%')) & (~col("product_list_lower").like('%pkg%')) & (~col("product_list_lower").like('%rt%+%'))) | 
                (col("package_booking").like('%CAR%')),'CAR')\
            .when(((col("pagename").isin('PAGE.ACTIVITIES', 'PAGE.GROUNDTRANSPORTATION', 'PAGE.LX-SEARCH', 'PAGE.LX-GT-SEARCH', 'PAGE.LX.INFOSITE.INFORMATION', 'PAGE.LX-GT.INFOSITE.INFORMATION', 'PAGE.LX.CHECKOUT.CONFIRMATION', 'PAGE.LX-GT.CHECKOUT.CONFIRMATION')) & (col("site_name") == 'travcompany.COM')) |
                ((col("LOB").isin('destination-discovery','destination-travelguides','local spert','lx','lx-travelguides')) & (col("site_name") == 'ORBITZ.COM')) |
                ((col("pagename").isin('PAGE.ACTIVITIES', 'PAGE.GROUNDTRANSPORTATION', 'PAGE.LX-SEARCH', 'PAGE.LX-GT-SEARCH', 'PAGE.LX.INFOSITE.INFORMATION', 'PAGE.LX-GT.INFOSITE.INFORMATION', 'PAGE.LX.CHECKOUT.CONFIRMATION', 'PAGE.LX-GT.CHECKOUT.CONFIRMATION')) & (col("site_name").isin('LASTMINUTE.AU','LASTMINUTE.NZ','WOTIF.NZ','WOTIF.AU'))) |
                ((col("LOB") == 'LOCAL spERT') & (col("Activity_type") == 'B')) |
                (col("package_booking").like('%LX%')),'TSHOP')
        )\
        .drop("product_list_lower")\
        .filter("product_ln_name is not null")
        
        
    creating_origin_destination = product_ln_name_creation.withColumn("origin_airpt_code", spr("(split(origin, ':'))[1]"))\
        .withColumn("dest_airpt_code", spr("(split(destination, ':'))[1]"))\
        .withColumn("dest_reg_id", F.when(col("destination").like('%:%'),spr("(split(destination, ':'))[2]")).otherwise(col("destination")))\
        .withColumn("dest_airport_code", coalesce(col("dest_airpt_code"), col("destination")))\
        .withColumn("origin_airport_code", coalesce(col("origin_airpt_code"),col("origin")))
        
        
    proper_origin_destination =  creating_origin_destination.withColumn("org_u", col("origin_airport_code"))\
        .withColumn("dest_u", F.when(((col("product_ln_name") == 'FLIGHT') | (col("product_ln_name") == 'CAR')), col("dest_airport_code")).otherwise(col("dest_reg_id")))\
        .drop("origin_airport_code").drop("dest_airport_code")\
        .withColumn("package_flag", F.when(col("LOB").like('%PACKAGE%'), 'package').otherwise('single'))


    geoDim = spark.read.parquet("s3://travcompany-term/project_traveler_profile/testSSS/geo_dim_automated/*").select("geo_id","city_geo_id","multi_city_vicinity_geo_id").withColumn("city_id",F.when(col("city_geo_id") !='-1',col("city_geo_id"))).withColumn("multicity_id",F.when(col("multi_city_vicinity_geo_id")!='-1',col("multi_city_vicinity_geo_id"))).select("geo_id","city_id","multicity_id").withColumn("mCity_city_air_id",coalesce("multicity_id","city_id"))
    airport_multi = spark.read.parquet("s3://travcompany-term/testSSS/vb/airport_multi/mapping/")
     

    agents = spark.read.csv("s3://travcompany-term/project_traveler_profile/testSSS/tmp/taap_agents_20180405.csv", header=True).select(col("tuid").cast(IntegerType()), col("tpid").cast(IntegerType()) ,col("spuserid").cast(IntegerType()))

    agent = agents.select(trim(col("tuid")).alias("tuid"), trim(col("tpid")).alias("travelproductid"))
    broadcast(agent)

    sp = spark.sql("select trim(tuid) as tuid, trim(travelproductid) as travelproductid, trim(spuserid) as spuser_id from sync.travelerspuser")

    agent_sp = agent.join(sp,(agent.tuid==sp.tuid) & (agent.travelproductid==sp.travelproductid),"inner").select("spuser_id").withColumn("agent", lit(1)).distinct()

    agent.unpersist()


    searches_2= proper_origin_destination.join(agent_sp, "spuser_id", "leftouter").select(proper_origin_destination["*"], "agent").filter("agent is NULL").drop("agent")

    searches_fc_dest = searches_2.filter("product_ln_name in ('CAR','FLIGHT')").join(airport_multi,searches_2.dest_u==airport_multi.airport_code,"leftouter").select(searches_2["*"],col("mCity_city_air_id").alias("dest_multi"))

    searches_ht_dest = searches_2.filter("product_ln_name in ('HOTEL','TSHOP')").join(geoDim,searches_2.dest_u==geoDim.geo_id,"leftouter").select(searches_2["*"],col("mCity_city_air_id").alias("dest_multi"))

    searches_3 = searches_fc_dest.unionAll(searches_ht_dest)

    searches_f_orig = searches_3.filter("product_ln_name in ('FLIGHT')").join(airport_multi,searches_3.org_u==airport_multi.airport_code,"leftouter").select(searches_3["*"],col("mCity_city_air_id").alias("orig_multi"))

    airport_multi.unpersist()


    origin = spark.read.parquet("s3://travcompany-term/project_traveler_profile/testSSS/tmp/rnz/ID_origin_mapping/*/*/*/").distinct()

    searches_hct_orig = searches_3.filter("product_ln_name in ('CAR','HOTEL','TSHOP')").withColumn("City_Country",concat(col("geo_city"),lit(":"),col("geo_country"))).join(origin,"City_Country","leftouter").select(searches_3["*"],col("MCity_City").alias("orig_multi")).drop("City_Country")

    searches_4 = searches_f_orig.unionAll(searches_hct_orig)

    geoDim2 = spark.read.parquet("s3://travcompany-term/project_traveler_profile/testSSS/geo_dim_automated/*").withColumn("country",upper(col("country_name"))).na.replace(['SOUTH KOREA'], ['KOREA'], 'country').distinct()
    broadcast(geoDim2)

    searches_5 = searches_4.join(geoDim2.select( col("geo_id").alias("dest_multi"),col("longitude").alias("dest_lon"), col("latitude").alias("dest_lat"),col("country_code").alias("dest_cntry_code"),col("country").alias("dest_country"),col("continent_name"),col("super_region_name")), "dest_multi", "left_outer")

    searches_6 = searches_5.join(geoDim2.select( col("geo_id").alias("orig_multi"),col("longitude").alias("org_lon"), col("latitude").alias("org_lat"),col("country_code").alias("orign_cntry_code"),col("country").alias("orign_country_temp")), "orig_multi", "left_outer")

    geoDim2.unpersist()

    site_lkp = spark.sql("select trim(site_name) as site_name,tpid,eapid,pos,trim(upper(cntry_name)) as pos_country from sync.site_tpid_lkp").distinct().dropDuplicates(["site_name", "tpid", "pos","pos_country"])
    broadcast(site_lkp)

    searches_7 = searches_6.join(site_lkp,"site_name","leftouter").withColumn("orign_country",coalesce(col("orign_country_temp"),col("pos_country"))).withColumn("Dom_Int", F.when(col("dest_country")==col("orign_country"),"DOM").otherwise("INT")).drop("orign_country_temp").withColumn("air_trip_type", parse_flight_type_udf("search_parameters", "product_ln_name")).drop("paid")

    site_lkp.unpersist()


    def lob(LOB, product_ln_name):
        if (LOB == None):
            return None
        elif ('TSHOP' in product_ln_name):
            return "TSHOP"
        elif ((LOB =='FLIGHT') or (LOB == 'FLIGHTS')):
            return "FLIGHT"
        elif ((LOB =='HOTEL') or (LOB == 'HOTELS') or (LOB == 'ITINERARY')):
            return "HOTEL"
        elif ((LOB == 'CAR') or (LOB== 'CARS')):
            return "CAR"
        elif ('LOCAL spERT' in LOB):
            return "TSHOP"
        elif (LOB == 'PACKAGE:FC'):
            return "FC"
        elif (LOB == 'PACKAGE:FH'):
            return "FH"
        elif (LOB == 'PACKAGE:HC'):
            return "HC"
        elif (LOB == 'PACKAGE:FHC'):
            return "FHC"
        else:
            return None

    lob_udf = udf(lob,StringType())


    global c
    c = 1
    global e
    e = "0000000000"

    def counter1(diff, diff2, sp, Activity_type, Dom_Int):
        global c
        global e
        if sp!=e:
            e = sp
            c = 1
        if Dom_Int == "DOM":
            if ((diff > 3) & (diff2 > 0)):
                c = c+1
            return(c)

        else:
            if ((diff > 5) & (diff2 > 0)):
                c = c+1
            return(c)

    trip_udf = udf(counter1, IntegerType())


   
    prev_base = spark.read.parquet(prev_path).filter(col("check_in").cast("Date").isNotNull() & col("check_out").cast("Date").isNotNull())

    old_trips = prev_base.filter(col("trip_end3")<(filter_date - td(days=8)))

    old_trips.drop("file_name").write.mode("overwrite").parquet("s3://travcompany-term/project_traveler_profile/testSSS/tmp/rnz/ID_hourly/historical/trips_super_final/" + str(date)[0:4] +"/" + str(date)[5:7]+"/"+str(date)[8:10]+"/")

    open_trips = prev_base.filter(col("trip_end3")>=(filter_date - td(days=8)))

    s = spark.read.parquet("s3://travcompany-term/project_traveler_profile/testSSS/tmp/rnz/hourly_transactions/" + str(date)[0:4] +"/" + str(date)[5:7]+"/"+str(date)[8:10]+"/").drop("upload_date")
    
    searches = s.withColumn("LOB_Type",lob_udf("LOB","product_ln_name"))\
        .withColumn("cancellation_flag", F.when(col("event_list").like('%,1,%'), 0).otherwise(None))\
        .drop("start_time").drop("end_time").drop("gmt_dtm2")\
        .withColumn("property_id_temp",
            when((col("lob").like('%PACKAGE:%')) & (col("Activity_type") == 'B'), coalesce((split(split(col("package_booking"), 'HOTEL:')[1],';')[0]), (split(split(col("package_booking"), 'LX-GT:')[1],';')[0]), (split(split(col("package_booking"), 'LX:')[1],';')[0]), (split(split(col("package_booking"), 'INSURANCE:')[1],';')[0])))\
            .when((col("lob").like('%PACKAGE:%')) & (col("Activity_type") == 'S'), coalesce((split(split(col("product_list"), 'HOTEL:')[1],';')[0]), (split(split(col("product_list"), 'LX-GT:')[1],';')[0]), (split(split(col("product_list"), 'LX:')[1],';')[0]), (split(split(col("product_list"), 'INSURANCE:')[1],';')[0])))\
            .when(~ col("lob").like('%PACKAGE:%'), coalesce((split(split(col("product_list"), 'HOTEL:')[1],';')[0]), (split(split(col("product_list"), 'LX-GT:')[1],';')[0]), (split(split(col("product_list"), 'LX:')[1],';')[0]), (split(split(col("product_list"), 'INSURANCE:')[1],';')[0]))))\
        .withColumn("property_id", when(((col("product_ln_name") == 'FLIGHT') | (col("product_ln_name") == 'CAR')), lit(None)).otherwise(col("property_id_temp"))).drop("property_id_temp")\
        .drop("package_booking")
        
    ## new property id logic on top of above logic
    corrected_pd  = searches.filter("property_id rlike '[A-Z]'").drop("property_id").withColumn("property_id" , F.when(col("product_ln_name") == 'TSHOP', coalesce((split(split(col("product_list"), 'LX-GT:')[1],';')[0]),(split(split(col("product_list"), 'LX:')[1],';')[0]))).when(col("product_ln_name") == 'HOTEL',coalesce((split(split(col("product_list"), 'HOTEL:')[1],';')[0]),(split(split(col("product_list"), 'INSURANCE:')[1],';')[0]))).otherwise(None))
    # do not change
    correct_pd = searches.filter("property_id not rlike '[A-Z]'")

    corrected_pd_null = corrected_pd.filter("property_id rlike '[A-Z]'").drop("property_id").withColumn("property_id", lit(None))

    corrected_pd_not_null = corrected_pd.filter("property_id not rlike '[A-Z]'")

    corrected_pd_final = corrected_pd_null.unionAll(corrected_pd_not_null.select(corrected_pd_null.columns))

    df_with_pd = correct_pd.unionAll(corrected_pd_final.select(correct_pd.columns))

    null_pd = searches.filter("property_id is null")
    
    searches = df_with_pd.unionAll(null_pd.select(df_with_pd.columns))
    
    open_trips_2 = open_trips.select(searches.columns)

    combined = open_trips_2.unionAll(searches.select(open_trips_2.columns)).filter(col("check_in").cast("Date").isNotNull() & col("check_out").cast("Date").isNotNull()).withColumn("los" , datediff("check_out", "check_in")).filter((((col("product_ln_name") == 'FLIGHT') | (col("product_ln_name") == 'CAR')) & (col("los") <= 330)) | (((col("product_ln_name") == 'HOTEL') | (col("product_ln_name") == 'TSHOP')) & (col("los") <= 40))).drop("los")

    c1 = combined.subtract(combined.filter("cancellation_flag=0"))
    c2 = combined.filter("cancellation_flag=0").drop("cancellation_flag")

    try:
        cancel = spark.read.parquet("s3://travcompany-term/project_traveler_profile/testSSS/cross_sell/cancellations/" + str(date) +"/").filter("trvl_acct_tpid is not NULL").withColumn("cancellation_flag",lit(1)).distinct()
        cancel.printSchema()

    omni_bookings = c2.withColumn("order_nbr", c2.purchaseid.substr(5, 13).cast(LongType()))
    order_nbr_cancels = omni_bookings.filter("trl = ''")
    trl_cancels = omni_bookings.filter("trl != ''")


    order_nbr_cancellations = order_nbr_cancels.join(cancel, (order_nbr_cancels.order_nbr == trim(cancel.order_nbr)) & (order_nbr_cancels.tpid == trim(cancel.trvl_acct_tpid)), "leftouter")\
        .select(order_nbr_cancels["*"], cancel.cancellation_flag).na.fill({'cancellation_flag': 0}).distinct().select(c1.columns)

    trl_cancellations = trl_cancels.join(cancel, (trl_cancels.trl == trim(cancel.trl)) & (trl_cancels.tpid == trim(cancel.trvl_acct_tpid)), "leftouter")\
        .select(trl_cancels["*"], cancel.cancellation_flag).na.fill({'cancellation_flag': 0}).distinct().select(c1.columns)

    cancel.unpersist()

    c3 = order_nbr_cancellations.unionAll(trl_cancellations.select(order_nbr_cancellations.columns)).drop("order_nbr")

    AllActivities = c1.unionAll(c3.select(c1.columns)).filter("dest_multi is not NULL")

    single_trip_lag = AllActivities.withColumn("diff", datediff(col("check_in"), F.lag("check_in",1,0).over(Window.partitionBy("spuser_id","dest_multi").orderBy("check_in", "check_out")))).withColumn("diff2", datediff(col("check_in"), F.lag("check_out",1,0).over(Window.partitionBy("spuser_id","dest_multi").orderBy("check_in", "check_out"))))

    Trips = single_trip_lag.withColumn("ID", concat(col("spuser_id"), lit("-"), col("dest_multi"), lit("-"), trip_udf("diff", "diff2", "spuser_id", "Activity_type", "Dom_Int"))).drop("diff2")

    window = Window.partitionBy("ID")

    trip_dates = Trips.select("*", F.min("check_in").over(window).alias("trip_start"), F.max("check_out").over(window).alias("trip_end"), unix_timestamp(F.min("date_time").over(window)).alias("timestamp"))

    trip_dates_2 = trip_dates.withColumn("elementary_row_no",F.row_number().over(Window.partitionBy("ID").orderBy("date_time")))

    window = Window.partitionBy("ID").orderBy("date_time")

    trip_dates_2 = trip_dates.select("ID","date_time","check_in","check_out",F.row_number().over(window).alias("row")).filter("row = 1").drop("row").withColumnRenamed("ID","ID_right_table").withColumnRenamed("trip_start","trip_start_right_table").withColumnRenamed("trip_end","trip_end_right_table").withColumnRenamed("check_in","first_activity_start_date").withColumnRenamed("check_out","first_activity_end_date").withColumnRenamed("date_time","date_time_right_table")

    trip_dates_join = trip_dates.join(trip_dates_2,trip_dates.ID == trip_dates_2.ID_right_table,'inner').drop("ID_right_table").drop("date_time_right_table")

    trip_dates_join = trip_dates_join.withColumn("ID_std", concat(col("spuser_id"), lit("-"), col("dest_multi"), lit("-"), unix_timestamp(concat( col("first_activity_start_date"), lit(" 00:00:00"))), lit("-"), unix_timestamp(concat( col("first_activity_end_date"), lit(" 00:00:00"))), lit("-"), col("timestamp"))).drop("first_activity_start_date").drop("first_activity_end_date")

   

    global c
    c = 1
    global e
    e = "0000000000"

    def counter2(diff,loc_diff, sp, Dom_Int):
        global c
        global e
        if sp!=e:
            e = sp
            c = 1
        if Dom_Int == "DOM":
            if (diff >= 0 and diff <= 30 and loc_diff<=5) :
                c = c
            else :
                c = c+1
            return(c)
        else:
            if (diff >= 0 and diff <= 30 and loc_diff<=7) :
                c = c
            else :
                c = c+1
            return(c)

    trip2_udf = udf(counter2, IntegerType())
    
    booked = ele_trip.filter("sum!=0").withColumn("ID2",concat(col("ID"),lit("-B"))).withColumn("trip_start2",col("trip_start")).withColumn("trip_end2",col("trip_end")).withColumn("timestamp2",col("timestamp")).withColumn("row_no_2",col("elementary_row_no")).withColumn("ID_std2",concat(col("ID_std"),lit("-B")))

    search_only = ele_trip.filter("sum=0")

    window = Window.partitionBy("ID_std")

    Trip2_sub = search_only.select("*", F.min("local_dt").over(window).alias("min_local"), F.max("local_dt").over(window).alias("max_local"))

    Trips = Trip2_sub.withColumn("diff", datediff(col("trip_start"), F.lag("trip_start",1,0).over(Window.partitionBy("spuser_id","dest_multi").orderBy("min_local","max_local","trip_start", "trip_end")))).withColumn("loc_diff", datediff(col("min_local"), F.lag("max_local").over(Window.partitionBy("spuser_id","dest_multi").orderBy("min_local","max_local","trip_start", "trip_end")))).withColumn("ID2", concat(col("spuser_id"), lit("-"), col("dest_multi"), lit("-"), trip2_udf("diff","loc_diff", "spuser_id", "Dom_Int"))).drop("diff").drop("loc_diff")

    window = Window.partitionBy("ID2")

    trip_dates = Trips.select("*", F.min("check_in").over(window).alias("trip_start2"), F.max("check_out").over(window).alias("trip_end2"), unix_timestamp(F.min("date_time").over(window)).alias("timestamp2")).withColumn("row_no_2",F.row_number().over(Window.partitionBy("ID2").orderBy("date_time")))

    window = Window.partitionBy("ID2").orderBy("date_time")

    trip_dates_2 = trip_dates.select("ID2","date_time","check_in","check_out","row_no_2").filter("row_no_2 = 1").drop("row_no_2").withColumnRenamed("ID2","ID_right_table").withColumnRenamed("check_in","first_activity_start_date").withColumnRenamed("check_out","first_activity_end_date").withColumnRenamed("date_time","date_time_right_table")

    trip_dates_join = trip_dates.join(trip_dates_2,trip_dates.ID2 == trip_dates_2.ID_right_table,'inner').drop("ID_right_table").drop("date_time_right_table")

    trip_dates_final = trip_dates_join.withColumn("ID_std2", concat(col("spuser_id"), lit("-"), col("dest_multi"), lit("-"), unix_timestamp(concat( col("first_activity_start_date"), lit(" 00:00:00"))), lit("-"), unix_timestamp(concat( col("first_activity_end_date"), lit(" 00:00:00"))), lit("-"), col("timestamp2"))).drop("first_activity_start_date").drop("first_activity_end_date").drop("min_local").drop("max_local")

    final_trips = booked.unionAll(trip_dates_final.select(booked.columns))
    
    global c2
    c2 = 1
    global e2
    e2 = "0000000000"

    def counter3(diff, out_in, origin, destination, sp, Dom_Int):
        global c2
        global e2
        if sp!=e2:
            e2 = sp
            c2 = 1

        if out_in<=3  or (out_in<7 and (origin==destination)):    # 3 average length of stay across all LOB , 7 average LOS of flight returns trip
            return c2
        else:
            if Dom_Int == "DOM":
                if (diff > 6 ):
                    c2 = c2+1
                return(c2)

            else:
                if (diff > 8 ):
                    c2 = c2+1
                return(c2)

    multi_udf = udf(counter3, IntegerType())

    trip_dates = spark.read.parquet("s3://travcompany-term/project_traveler_profile/testSSS/tmp/rnz/ID_hourly/open/trips2_same_activity_date/" + str(date)[0:4] +"/" + str(date)[5:7]+"/"+str(date)[8:10]+"/*")

    upload_date = date + td(days=2)

    w = Window.partitionBy("ID_std2","product_ln_name", "check_in", "check_out")
    w_1 = Window.partitionBy("ID_std2","product_ln_name").orderBy(desc("weight_final"))
    w_2 = Window.partitionBy("ID_std2","product_ln_name")
    w_3 = Window.partitionBy("ID_std2")

    # Get recommended dates based on recency frequency

    trip_dates_temp = trip_dates\
    .withColumn("curr_date", lit(upload_date))\
    .withColumn("diff_from_today_date", datediff("curr_date","local_dt"))\
    .withColumn("weight_dummy", 1-(col("diff_from_today_date")/14))\
    .withColumn("weight_new", F.when(col("weight_dummy") < 0.0, 0.0714285714285714).otherwise(col("weight_dummy")))\
    .withColumn("weight_final", F.sum("weight_new").over(w))

    trip_dates_temp_2 = trip_dates_temp.withColumn("rank", F.row_number().over(w_1)).\
    withColumn("recommended_outbound_temp", F.when(col("rank") == 1, col("check_in")).otherwise(None)).\
    withColumn("recommended_inbound_temp", F.when(col("rank") == 1, col("check_out")).otherwise(None)).\
    withColumn("recommended_outbound", F.max("recommended_outbound_temp").over(w_2)).\
    withColumn("recommended_inbound", F.max("recommended_inbound_temp").over(w_2))



    trip_dates_temp_3 = trip_dates_temp_2.withColumn("min_recommended_outbound", F.min("recommended_outbound").over(w_3)).\
    withColumn("max_recommended_inbound", F.max("recommended_inbound").over(w_3))




    trip_lag = trip_dates_temp_3.drop("flag").drop("sum").withColumn("diff", F.datediff(col("min_recommended_outbound"), F.lag("min_recommended_outbound",1,0).over(Window.partitionBy("spuser_id").orderBy("min_recommended_outbound", "max_recommended_inbound")))).withColumn("out_in", F.datediff(col("min_recommended_outbound"), F.lag("max_recommended_inbound",1,0).over(Window.partitionBy("spuser_id").orderBy("min_recommended_outbound", "max_recommended_inbound")))).withColumn("prev_dest", F.lag("dest_multi",1,0).over(Window.partitionBy("spuser_id").orderBy("min_recommended_outbound", "max_recommended_inbound"))).drop("curr_date","diff_from_today_date","weight_dummy","weight_new","weight_final","rank","recommended_outbound_temp","recommended_inbound_temp","recommended_outbound","recommended_inbound","min_recommended_outbound","max_recommended_inbound")
  
    multi_trips_temp = trip_lag.withColumn("superID", concat(col("spuser_id"), lit("-"), multi_udf("diff", "out_in", "orig_multi", "prev_dest","spuser_id","Dom_Int"))).drop("out_in").drop("prev_dest").drop("diff")

    window = Window.partitionBy("superID")

    multi_trips = multi_trips_temp.select("*", F.min("check_in").over(window).alias("trip_start3"), F.max("check_out").over(window).alias("trip_end3"),F.min("date_time").over(window).alias("first_activity3")).withColumn("timestamp3",unix_timestamp("first_activity3")).withColumn("super_row_no",F.row_number().over(Window.partitionBy("superID").orderBy("date_time")))

    multi_trips_2 = multi_trips.select("superID","date_time","check_in","check_out","super_row_no").filter("super_row_no = 1").drop("super_row_no").withColumnRenamed("superID","superID_right_table").withColumnRenamed("check_in","first_activity_start_date").withColumnRenamed("check_out","first_activity_end_date").withColumnRenamed("date_time","date_time_right_table")

    multi_trips_join = multi_trips.join(multi_trips_2,multi_trips.superID == multi_trips_2.superID_right_table,'inner').drop("superID_right_table").drop("date_time_right_table")


    multi_dates_join = multi_trips_join.withColumn("superID_std", concat(col("spuser_id"), lit("-"), unix_timestamp(concat( col("first_activity_start_date"), lit(" 00:00:00"))), lit("-"), unix_timestamp(concat( col("first_activity_end_date"), lit(" 00:00:00"))), lit("-"), col("timestamp3"))).drop("first_activity_start_date").drop("first_activity_end_date").filter(col("trip_end3")>=(filter_date - td(days=8)))

   
    attach_data = spark.read.parquet(path).withColumn("Activity_type", F.when(col("event_list").like('%,1,%'), "B").otherwise("S")).filter("Activity_type=='B'").distinct().withColumnRenamed("local_dt","local_date").withColumn("end_use_date_key",col("check_out").cast("Date")).withColumn("begin_use_date_key",col("check_in").cast("Date")).withColumnRenamed("orig_multi","orign_multi").withColumnRenamed("orign_cntry_code","origin_cntry_code").withColumnRenamed("destination","dest").select("spuser_id","guid","tuid","local_date","gmt_dtm","product_ln_name","orign_multi","dest_multi","origin_cntry_code","dest_cntry_code","continent_name","super_region_name","Dom_Int","begin_use_date_key","end_use_date_key","search_window","trip_duration","geo_city","geo_country","dest","org_lon","org_lat","dest_lon","dest_lat","trl","tpid","eapid","air_trip_type","pos","pos_country","package_flag","LOB","Activity_type", "check_out").drop("check_out").drop("check_in")

    attach_data = attach_data.filter("end_use_date_key is not null and begin_use_date_key is not null")
   


 

    trips = spark.read.parquet("s3://travcompany-term/project_traveler_profile/testSSS/tmp/rnz/ID_hourly/open/trips_super_final/" + str(date)[0:4] + "/" + str(date)[5:7]+ "/" +str(date)[8:10]+ "/*").filter("cancellation_flag != 1 or cancellation_flag is null")

    win_ID = Window.partitionBy("ID_std")
    win_trip_tpid = Window.partitionBy("ID_std", "tpid")

    trip_tpid_1 = trips.select("ID_std", "tpid", "Activity_type", "elementary_row_no").withColumn("booked_flag", F.when(col("Activity_type") == 'B', 1).otherwise(0)).withColumn("booked_trip", F.max("booked_flag").over(win_ID)).filter("tpid is not null").filter(trim(col("tpid")) != '')

    booked_trip_tpid = trip_tpid_1.filter("booked_trip > 0").filter("Activity_type = 'B'")\
        .withColumn("row_number", F.row_number().over(win_ID.orderBy(desc("elementary_row_no"))))\
        .filter("row_number = 1").select("ID_std", "tpid")

    non_booked_trip_tpid = trip_tpid_1.filter("booked_trip = 0").select("ID_std", "tpid")\
        .withColumn("tpid_search_count", F.count("tpid").over(win_trip_tpid)).distinct()\
        .withColumn("most_searched_tpid", F.row_number().over(win_ID.orderBy(desc("tpid_search_count")))).filter("most_searched_tpid = 1")\
        .select("ID_std", "tpid")

    

