#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import boto3
import json
from boto3.dynamodb.conditions import Key, Attr
from pprint import pprint

dynamodb = boto3.resource('dynamodb')
table    = dynamodb.Table('val00362_1')

res = table.get_item(
    Key={
        "id": 3
    },
    #ConsistentRead=True
)
item = res["Item"]

pprint(res)
print("num = [%s]" % item["num"])
print("str = [%s]" % item["str"])
print("in strset:")
for i in item["strset"]:
    print("[%s]" % i)
#counts = json.loads(res["Item"]["question"])
#pprint(counts)
