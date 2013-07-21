dynamomite
==========

A "simple" abstraction layer to Amazon's DynamoDB.

Dynamomite turns DynamoDB into "dict-like" Python objects. I like DynanamoDB. It's free tier works out well for small projects; plus, you can pay for more. The standard boto dynamo interface is straightforward enough ... but I enjoy things as simple as possible.

Why? ... I dunno, it seemed like a good idea at the time.

#Usage#

An example:
```python

import dynamomite

dynamodb_connection = boto.dynamodb.connect_to_region('us-east-1', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
users = dynamomite.Table(dynamodb_connection, "users")

print users['yodawg@gmail.com']['password']

```
