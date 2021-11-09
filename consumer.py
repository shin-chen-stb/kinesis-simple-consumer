from boto import kinesis
import time
import sys

id=sys.argv[0]
key=sys.argv[1]
streamName = sys.argv[2]
auth = {"aws_access_key_id":id, "aws_secret_access_key":key}
connection = kinesis.connect_to_region('ap-northeast-1',**auth)


shard_ids = []
response = connection.describe_stream(streamName)
stream_name = response['StreamDescription']['StreamName']                   
print(response)
for shard_id in response['StreamDescription']['Shards']:
     shard_id = shard_id['ShardId']
     print(shard_id)
     shard_iterator = connection.get_shard_iterator(stream_name, shard_id, 'TRIM_HORIZON')
     #shard_iterator = connection.get_shard_iterator(stream_name, shard_id, 'LATEST')
     shard_ids.append({'shard_id' : shard_id ,'shard_iterator' : shard_iterator['ShardIterator'] })

tries = 0
limit = 100
result = []
print(shard_iterator)
shard_iterator = shard_iterator['ShardIterator']
while tries < 100:
     time.sleep(1)
     tries += 1
     response = connection.get_records(shard_iterator = shard_iterator)
     print(response)
     shard_iterator = response['NextShardIterator']
     if len(response['Records'])> 0:
          print(response)
          for res in response['Records']: 
               result.append(res)   
print(result)
