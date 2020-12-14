#!/usr/bin/env python3
import json
from decimal import Decimal
from datetime import date, datetime
import boto3
from botocore.exceptions import ClientError
import asyncio

#################################
##
def json_default_encoder(obj):
    if isinstance(obj, Decimal):
        if obj % 1 > 0:
           return float(obj)
        else:
            return int(obj)
    elif isinstance(obj, (datetime, date)):
        return obj.isoformat()

    raise TypeError("Object of type '%s' is not JSON serializable" % type(obj).__name__)


############################
fr_config = None
patrols = None
summary = None
TROOP_ORDER_OWNER = 'troop27summary'

############################
#
async def load_fr_config(s3):
    global fr_config

    if not fr_config:
        print("Loading fr_config from s3")
        obj = s3.Object('t27fundraiser', 'T27FundraiserConfig.json')
        fr_config = json.loads(obj.get()['Body'].read())


############################
#
async def load_patrol_map(s3):
    global patrols

    if not patrols:
        print("Loading patrol map from s3")
        obj = s3.Object('t27fundraiser', 'T27FundraiserUserMap.json')
        patrols = json.loads(obj.get()['Body'].read())

############################
#
async def load_summary(table):
    global summary

    if not summary:
        print("Loading summary from dynamodb")
        resp = table.scan()
        #print(json.dumps(resp, indent=2, default=json_default_encoder))
        for idx, item in enumerate(resp['Items']):
            for k in item.keys():
                if 'orderOwner'==k: continue
                if 'amountSold'==k or 'donation'==k:
                    resp['Items'][idx][k] = float(item[k])
                else:
                    resp['Items'][idx][k] = int(item[k])

        summary = resp['Items']


############################
#
async def load_cache_data(s3, db):
    await asyncio.gather(
        load_summary(db),
        load_fr_config(s3),
        load_patrol_map(s3))


############################
#
def extract_vals(dbrec, image):
    vals = {
        "orderOwner": dbrec['Keys']['orderOwner']['S'],
        "amountSold": float(image['amountTotal']['N']),
        "donation": 0.0,
    }

    if 'mulch' == fr_config['kind']:
        vals['bags'] = 0
        vals['spreading'] = 0

    for k,v in image['orderByDelivery']['M'].items():
        if 'donation'==k:
            vals['donation'] = vals['donation'] + float(v['M']['amountDue']['N'])

            if 'mulch' != fr_config['kind']:
                # Only care about donations and not sure if not mulch specific
                # so break if we get a donation and it isn't mulch
                break
            continue

        if 'mulch' != fr_config['kind']:
            continue

        #This is mulch so we need to get spreading and bags
        items = v['M']['items']['M']
        if 'bags' in items:
            vals['bags'] = vals['bags'] + int(items['bags']['N'])

        if 'spreading' in items:
            vals['spreading'] = vals['spreading'] + int(items['spreading']['N'])

    return(vals)


############################
#
def add_or_insert(table, a_summary_item):
    update_expr = []
    expr_attr = {}
    for k, v in a_summary_item.items():
        if 'orderOwner' == k: continue
        update_expr.append(f'{k}=:{k}')
        expr_attr[f':{k}'] = str(v)

    update_expr = f'SET {", ".join(update_expr)}'

    #print(f"Update Expr: {update_expr}")
    #print(f"ExprAttr: {json.dumps(expr_attr, default=json_default_encoder)}")
    try:
        table.update_item(
            Key={
                'orderOwner': a_summary_item['orderOwner']
            },
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_attr,
            ReturnValues="NONE"
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
        raise

############################
#
def get_summaries(owner, patrol, troop):
    def gen_default(owner_id):
        defaults = {
            "orderOwner": owner_id,
            "amountSold": 0.0,
            "donation": 0.0,
        }

        if 'mulch' == fr_config['kind']:
            defaults['bags'] = 0
            defaults['spreading'] = 0
        return defaults



    owner_summary = (gen_default(owner), -1)
    patrol_summary = (gen_default(patrol), -1)
    troop_summary = (gen_default(troop), -1)

    for idx, user_summary in enumerate(summary):
        if user_summary['orderOwner']==owner:
            owner_summary = (user_summary, idx)
        if user_summary['orderOwner']==troop:
            troop_summary = (user_summary, idx)
        if patrol and user_summary['orderOwner']==patrol:
            patrol_summary = (user_summary, idx)

    return [owner_summary, patrol_summary, troop_summary]

############################
#
def no_needed_vals_changed(old, new):
    is_no_change = (old['amountSold'] == new['amountSold'] and
                    old['donation'] == new['donation'])

    if 'mulch' != fr_config['kind']:
        return(is_no_change)

    return(is_no_change and
           old['bags'] == new['bags'] and
           old['spreading'] == new['spreading'])


############################
#
def process_owner_summary_changes(table, summary_tuple, old_vals, new_vals):
    global summary

    owner_summary, idx = summary_tuple

    if old_vals:
        owner_summary['amountSold'] = owner_summary['amountSold'] - old_vals['amountSold']
        owner_summary['donation'] = owner_summary['donation'] - old_vals['donation']
        if 'mulch' == fr_config['kind']:
            owner_summary['bags'] = owner_summary['bags'] - old_vals['bags']
            owner_summary['spreading'] = owner_summary['spreading'] - old_vals['spreading']

    if new_vals:
        owner_summary['amountSold'] = owner_summary['amountSold'] + new_vals['amountSold']
        owner_summary['donation'] = owner_summary['donation'] + new_vals['donation']
        if 'mulch' == fr_config['kind']:
            owner_summary['bags'] = owner_summary['bags'] + new_vals['bags']
            owner_summary['spreading'] = owner_summary['spreading'] + new_vals['spreading']

    if -1 == idx:
        summary.append(owner_summary)
    else:
        summary[idx] = owner_summary
    add_or_insert(table, owner_summary)


############################
#
def get_patrol_name(order_owner):
    for name, members in patrols.items():
        if order_owner in members:
            return(name)
    return('DEFAULT')


############################
#
def process_record(table, rec):
    #print(f"Stream Record: {json.dumps(record, default=json_default_encoder)}")
    op = rec['eventName']
    dbrec = rec['dynamodb']

    old_vals = None
    if 'REMOVE' == op or 'MODIFY' == op:
        old_vals = extract_vals(dbrec, dbrec['OldImage'])
        #print(f"Old Vals: {json.dumps(old_vals, default=json_default_encoder)}")

    new_vals = None
    if 'INSERT' == op or 'MODIFY' == op:
        new_vals = extract_vals(dbrec, dbrec['NewImage'])
        #print(f"New Vals: {json.dumps(new_vals, default=json_default_encoder)}")

    if 'MODIFY' == op and no_needed_vals_changed(old_vals, new_vals):
        #print("No Changes needed so skipping")
        return False

    order_owner = (new_vals if new_vals else old_vals)['orderOwner']

    summaries = get_summaries(order_owner, get_patrol_name(order_owner), TROOP_ORDER_OWNER)

    for summary in summaries:
        process_owner_summary_changes(table, summary, old_vals, new_vals)

    return True


############################
#
def generate_summary_report():
    pass

############################
#
def handle_event(event):
    s3 = boto3.resource('s3')
    db = boto3.resource('dynamodb')
    table = db.Table('T27FundraiserSummary')

    # Load cache data from different sources
    asyncio.run(load_cache_data(s3, table))

    #print(json.dumps(event, indent=2, default=json_default_encoder))
    is_summary_changed = False
    for rec in event['Records']:
        if process_record(table, rec): is_summary_changed = True

    if is_summary_changed:
        generate_summary_report()

#################################
##
def lambda_handler(event, context):
    handle_event(event)

#################################
##
if __name__ == '__main__':
    #for fn in ['OnInsert', 'OnInsert2', 'OnRemove', 'OnInsert3', 'OnInsert4', 'OnModify']:
    if False:
        for fn in ['OnInsert', 'OnInsert2']:
            with open(f"{fn}.json", 'rb') as f:
                handle_event({ "Records": [json.load(f)] })
                out = json.dumps(summary, indent=2, default=json_default_encoder)
                print(f"****************\n{out}\n*******************")

    if True:
        for fn in ['OnRemove', 'OnInsert3', 'OnInsert4', 'OnModify']:
            with open(f"{fn}.json", 'rb') as f:
                handle_event({ "Records": [json.load(f)] })
                out = json.dumps(summary, indent=2, default=json_default_encoder)
                print(f"****************\n{out}\n*******************")
