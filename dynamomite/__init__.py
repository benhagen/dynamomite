#!/usr/bin/env python

# Made with the unfortunate belief that everything in Python should be iterable and dict-like

import boto.dynamodb2
import boto.dynamodb2.table
import boto.dynamodb2.items
import logging
from boto.dynamodb2.fields import HashKey
from time import time
import arrow
import re

logger = logging.getLogger("dynomomite")
logger.addHandler(logging.NullHandler())


class Item(object):

	def __init__(self, table, item=None, hash_key=None):
		self.table = table
		self.new = False
		self._loaded = int(time())
		if item:
			# Pre-existing entry
			self.item = item
		else:
			# New entry
			self.new = True
			self.item = boto.dynamodb2.items.Item(self.table.table, data={self.table.__hash_key__: hash_key})
			for key, value in self.table.__defaults__.items():
				self.__setitem__(key, value)
		return

	def __setitem__(self, key, value):
		if key not in self.table.__schema__.keys():
			raise LookupError
		# Sets are basically lists ... lets make them fudgable
		if self.table.__schema__[key] == set and type(value) == list:
			value = set(value)
		if type(value) != self.table.__schema__[key]:
			raise ValueError
		if type(value) == arrow.arrow.Arrow:
			self.item[key] = int(value.timestamp)
		else:
			self.item[key] = value

	def __getitem__(self, key):
		if key not in self.table.__schema__.keys():
			raise LookupError
		value = self.item[key]
		if self.table.__schema__[key] == arrow.arrow.Arrow:
			value = arrow.get(value)
		return value

	def __delitem__(self, key):
		del self.item[key]
		return

	def delete(self):
		self.item.delete()
		return

	def write(self, overwrite=True, partial=True, force=False):
		return self.item.save(overwrite=overwrite)
		if self.item.needs_save() or force:
			if partial and not self.new:
				return self.item.partial_save()
			else:
				return self.item.save(overwrite=overwrite)

	# Write and Save do the same thing
	def save(self, *args, **kwargs):
		return self.write(*args, **kwargs)

	def values(self):
		return self.item.values()


	def keys(self):
		return self.item.keys()

	def items(self):
		return self.item.items()

	def __str__(self):
		return "<Item[\"%s\"]>" % (self.__getitem__("id"))


class Table(object):
	__schema__ = {}
	__defaults = {}
	__sensitive__ = []
	__modifiable__ = []

	def __init__(self, region="us-east-1", aws_access_key_id=None, aws_secret_access_key=None, connection=None, item_class=Item):
		self._iter_current_results = None
		self._cache = {}
		self._cache_timeout = 5
		self.item_class = item_class
		self.len_expensive = True
		# If provided use the pre-existing connections, otherwise connect
		if connection:
			self.dynamo = connection
		else:
			self.dynamo = boto.dynamodb2.connect_to_region(region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
		self.schema = [HashKey(self.__hash_key__),]
		self.indexes = None
		self.table = boto.dynamodb2.table.Table(self.__table__, schema=self.schema, indexes=self.indexes, connection=self.dynamo)
		return

	def create(self, read=1, write=1):
		self.table.create(self.__table__, self.schema, throughput={'read': read, 'write': write}, connection=self.dynamo)

	def delete(self):
		return self.table.delete()

	def get(self, key, usecache=True):
		# Check that the provided key is of the right type or cast'able to the right type
		if type(key) != self.__schema__[self.__hash_key__]:
			self.__schema__[self.__hash_key__](key)
		if usecache and key in self._cache:
			if self._cache[key]._loaded + self._cache_timeout > time():
				return self._cache[key]
		kw = {self.__hash_key__: key}
		result = self.table.get_item(**kw)
		if len(result.keys()) == 0:
			# Not caching None results for now
			return None
		self._cache[key] = self.item_class(table=self, item=result, hash_key=key)
		return self._cache[key]

	def new(self, hash_key):
		# Should this be cached?
		result = self.get(hash_key, usecache=False)
		if result:
			raise LookupError
		return self.item_class(table=self, hash_key=hash_key)

	def next(self):
		""" Wrapper around full table scans """
		if not self._iter_current_results:
			self._iter_current_results = self.table.scan()
		try:
			item = next(self._iter_current_results)
		except StopIteration:
			raise StopIteration
		return self.item_class(table=self, item=item)

	def __iter__(self):
		return self

	def __len__(self, expensive=None):
		if expensive == None:
			expensive = self.len_expensive
		if not expensive:
			# Updated every 6 hours ... or so
			return int(self.table.count())
		else:
			# Full table scan, for the defining count consumer
			return int(self.dynamo.scan(table_name=self.__table__, select="COUNT")['Count'])

	def __getitem__(self, key):
		result = self.get(key)
		if result:
			return result
		else:
			raise LookupError

	def __contains__(self, key):
		result = self.get(key)
		if not result:
			return False
		return True

	def __delitem__(self, key):
		item = self.__getitem__(key)
		return item.delete()

#
# Types
#

EMAILADDRESS_REGEX = re.compile(r"[^@ ]+@[^@ ]+\.[^@ ]+")

class EmailAddress():
	def __init__(self, address):
		self.value = address
		if not self.validate():
			raise ValueError

	def validate(self):
		if not EMAILADDRESS_REGEX.match(self.value):
			return False
		return True

def TableCreate(dynamodb_connection, table_name, hash_key_name, hash_key_proto_value=str):
	schema = dynamodb_connection.create_schema(hash_key_name=hash_key_name, hash_key_proto_value=hash_key_proto_value)
	table = dynamodb_connection.create_table(name=table_name, schema=schema, read_units=1, write_units=1)
	return 

if __name__ == "__main__":
	print "Nothing here. Go away."



