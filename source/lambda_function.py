# --- coding: utf-8 ---
# 検索ネコ
# WebからAPI Gateway経由で呼ばれるLambda
# h3インデックスをDynamoDBから検索する。

import sys
import json
import os
import re
import requests
import h3
from urllib.parse import urljoin
from retry import retry
import boto3
from boto3.dynamodb.conditions import Key
import googlemaps
from geopy.distance import geodesic

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

DYNAMODB_NAME               = ""    if("DYNAMODB_NAME" not in os.environ)           else os.environ["DYNAMODB_NAME"]
LOCOGUIDE_API_ADDRESS       = ""    if("LOCOGUIDE_API_ADDRESS" not in os.environ)   else os.environ["LOCOGUIDE_API_ADDRESS"]
LOCOGUIDE_API_TOKEN         = ""    if("LOCOGUIDE_API_TOKEN" not in os.environ)     else os.environ["LOCOGUIDE_API_TOKEN"]
APIKEY_GOOGLE_MAP           = ""    if("APIKEY_GOOGLE_MAP" not in os.environ)       else os.environ["APIKEY_GOOGLE_MAP"]
RESULT_COUNT_MAX            = 100

DYNAMO_TABLE = boto3.resource("dynamodb").Table(DYNAMODB_NAME)
GMAPS        = googlemaps.Client(key=APIKEY_GOOGLE_MAP)

def lambda_handler(event, context):
    try:
        logger.info("=== START ===")
        types = event["queryStringParameters"]["type"]
        latlon = None
        if "latlon" in event["queryStringParameters"]:
            latlon = event["queryStringParameters"]["latlon"]
        address = None
        if "address" in event["queryStringParameters"]:
            address = event["queryStringParameters"]["address"]
        zoom = 16
        if "zoom" in event["queryStringParameters"]:
            zoom = int(event["queryStringParameters"]["zoom"])
        count = RESULT_COUNT_MAX
        if "count" in event["queryStringParameters"]:
            count = int(event["queryStringParameters"]["count"])
        isSort = False
        if "sort" in event["queryStringParameters"]:
            isSort = str2bool(event["queryStringParameters"]["sort"])
        
        if not latlon and address:
            latlon = getLatLonFromAddress(address)

        message = "type={0}, latlon={1}, address={2}, zoom={3}, count={4}, sort={5}".format(types, latlon, address, zoom, count, isSort)
        logger.info(message)
        
        hits = search_h3(types, latlon, zoom, count, isSort)

        result = {}
        has_clowd = False
        resultList = []
        locolist = []
        lastLat = 0
        lastLng = 0
        index = 0
        for item in hits:
            data = convert(types, item)
            
            if "locoguide_id" in item and item["locoguide_id"]:
                data["list"][0]["locoguide_id"] = item["locoguide_id"]
                data["list"][0]["crowd_lv"] = 0
                locolist.append(data["list"][0])

            if data["position"]["lat"] == lastLat and data["position"]["lng"] == lastLng:
                resultList[-1]["list"].append(data["list"][0])
            else:
                resultList.append(data)
            lastLat = data["position"]["lat"]
            lastLng = data["position"]["lng"]
            index = index + 1

        #ロコリストで混雑レベルを取得する
        if len(locolist) > 0:
            has_clowd = getCrowdLvFromLoco(resultList, locolist)

        result["list"] = resultList
        result["has_clowd"] = has_clowd
        
        return {
            "headers": {
                "Access-Control-Allow-Origin" : "*",
                "Access-Control-Allow-Credentials": "true"
            },
            'statusCode': 200,
            "body": json.dumps(result, ensure_ascii=False, indent=2)
        }

    except Exception as e:
        logger.exception(e)
        return {
            "statusCode": 500,
            "body": "error"
        }

@retry(tries=3, delay=1)
def getLatLonFromAddress(address):
    try:
        latlon = None
        result = GMAPS.geocode(address)
        try:
            logger.info(result)
            lat = result[0]["geometry"]["location"]["lat"]
            lng = result[0]["geometry"]["location"]["lng"]
            latlon = "{0},{1}".format(lat, lng)
        except Exception as e:
            logger.warn("can't get geocode : {0}".format(address))
            
        return latlon
    except Exception as e:
        logger.error(address)
        logger.exception(e)

def search_h3(types, latlon, zoom, count, isSort):
    if zoom <= 13:
        h3type = 7
    elif zoom >= 14 and zoom <= 15:
        h3type = 8
    else:
        h3type = 9

    latlon_ary = latlon.split(",")
    h3index = h3.geo_to_h3(float(latlon_ary[0]), float(latlon_ary[1]), h3type)
    rings = h3.k_ring_distances(h3index, 2)
    logger.info(rings)

    results = []
    ringIndex = 0
    for sdata in rings:
        for h3index in sdata:
            response = query_dynamodb(types, h3index, h3type)
            results.extend(response)
        logger.info("ringIndex={0}, resultCount={1}".format(ringIndex, len(results)))
        if len(results) >= count and ringIndex > 0:
            break
        ringIndex = ringIndex + 1
        
    logger.info("result size = {0}".format(len(results)))

    results = map(lambda data: calcDistanceFrom(latlon, data), results)
    results = list(results)
    
    if isSort:
        results = sorted(results, key=lambda data: data["distance"])
        if count > 0 and len(results) > count:
            results = results[0 : count]
    
    return results

def calcDistanceFrom(latlon, data):
    latlon_ary1 = latlon.split(",")
    latlon_ary2 = data["latlon"].split(",")
    distance = geodesic(latlon_ary1, latlon_ary2).km
    data["distance"] = distance
    return data

@retry(tries=3, delay=1)
def query_dynamodb(types, h3index, h3type):
    result_list = []
    keystr = "h3-{0}".format(h3type)

    if h3type == 9:
        response = DYNAMO_TABLE.query(
            KeyConditionExpression=Key("type").eq(types) & Key(keystr).begins_with(h3index)
        )
        result_list = response["Items"]
        while "LastEvaluatedKey" in response:	
            response = DYNAMO_TABLE.query(	
                KeyConditionExpression=Key("type").eq(types) & Key(keystr).begins_with(h3index),	
                ExclusiveStartKey = response["LastEvaluatedKey"])
            result_list.extend(response["Items"])     
            break
    else:
        indexstr = "LSI_type_h3-{0}".format(h3type)
        response = DYNAMO_TABLE.query(
            IndexName=indexstr,
            KeyConditionExpression=Key("type").eq(types) & Key(keystr).eq(h3index)
        )
        result_list = response["Items"]
        while "LastEvaluatedKey" in response:	
            response = DYNAMO_TABLE.query(
                IndexName=indexstr,	
                KeyConditionExpression=Key("type").eq(types) & Key(keystr).eq(h3index),	
                ExclusiveStartKey = response["LastEvaluatedKey"])	
            result_list.extend(response["Items"])
            break
        
    return result_list

def getCrowdLvFromLoco(resultlist, locolist):
    has_clowd = False
    try:
        ids = ""
        for item in locolist:
            ids += "{0},".format(item["locoguide_id"])
        ids.rstrip(",")
            
        url = LOCOGUIDE_API_ADDRESS + "?id=" + ids
        idList = []
        lvList = []
        has_clowd = requestLoco(url, 1, idList, lvList)

        for i in range(len(idList)):
            id = idList[i]
            lv = lvList[i]
            for tmp in locolist:
                if tmp["locoguide_id"] == id:
                    tmp["crowd_lv"] = lv
                    break

    except Exception as e:
        logger.exception(e)

    return has_clowd

def requestLoco(url, page, idList, lvList):
    logger.info("------loco address------page=" + str(page))
    logger.info(url)
    headers ={}
    headers["Authorization"] = "Bearer " + LOCOGUIDE_API_TOKEN
    response = request(url, headers)
    response.encoding = response.apparent_encoding
    content = response.content.decode("utf-8")
    jsn = json.loads(content)
    
    logger.info("list size = " + str(len(jsn)))
    logger.info(jsn)

    has_clowd = False
    for tmp in jsn:
        if "crowd_lamp" not in tmp or not tmp["crowd_lamp"]:
            continue
        color = tmp["crowd_lamp"]["color"]
        
        # ★TEST
        # ロコIDがNULLでもblueにするテストコード(↑これはコメントする)
        #if "crowd_lamp" not in tmp:
        #    continue
        #color = "blue"
        
        if tmp["crowd_lamp"] != None:
            color = tmp["crowd_lamp"]["color"]
        lv = 0
        if color == "red":
            lv = 3
        elif color == "yellow":
            lv = 2
        elif color == "green" or color == "blue":
            lv = 1
        lvList.append(lv)
        idList.append(str(tmp["id"]))
        has_clowd = True

    if "Link" in response.headers:
        nextUrl = response.headers["Link"]
        nextUrl = nextUrl[1:nextUrl.find(">")]
        page = page + 1
        flg = requestLoco(nextUrl, page, idList, lvList)
        if flg:
            has_clowd = True

    return has_clowd

@retry(tries=3, delay=1)
def request(url, headers):
    return requests.get(url, headers=headers)

def convert(types, item):
    data = {}
    data["type"] = types
    latlon = item["latlon"].split(",")
    data["position"] = { "lat": float(latlon[0]), "lng": float(latlon[1]) }
    
    child = {}
    child["guid"] = item["h3-9"]
    child["title"] = item["title"]
    child["tel"] = item["tel"]
    child["address"] = item["address"]
    child["distance"] = item["distance"]

    if "image" in item and item["image"]:
        child["image"] = urljoin("https://near-near-map.s3-ap-northeast-1.amazonaws.com/", item["image"])
    else:
        child["image"] = ""

    child["facebook"] = item["facebook"]
    child["twitter"] = item["twitter"]
    child["instagram"] = item["instagram"]
    
    has_xframe_options = [0, 0, 0, 0, 0, 0]
    has_xframe_options = item["has_xframe_options"].split(",")
    
    child["homepage"] = {
        "address": item["homepage"],
        "has_xframe_options": has_xframe_options[0]
    }
    child["media1"] = {
        "address": item["media1"],
        "has_xframe_options": has_xframe_options[1]
    }
    child["media2"] = {
        "address": item["media2"],
        "has_xframe_options": has_xframe_options[2]
    }
    child["media3"] = {
        "address": item["media3"],
        "has_xframe_options": has_xframe_options[3]
    }
    child["media4"] = {
        "address": item["media4"],
        "has_xframe_options": has_xframe_options[4]
    }
    child["media5"] = {
        "address": item["media5"],
        "has_xframe_options": has_xframe_options[5]
    }
    
    if "star" in item:
        child["star"] = int(item["star"])
    else:
        child["star"] = 0
    
    data["list"] = []
    data["list"].append(child)
    
    return data
    
def str2bool(s):
     return s.lower() in ["true", "t", "yes", "1"]