dynomomite
==========

A "simple" abstraction layer to Amazon's DynamoDB

Dynomomite turns DynamoDB into "dict-like" Python objects.

Why? ... I dunno, it seemed like a good idea at the time.

#Usage#

An example:
```python

import dynamomite

dynamodb_connection = boto.dynamodb.connect_to_region('us-east-1', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
users = dynamomite.table(dynamodb_connection, "users")

print users['yodawg@gmail.com']['password']

```
