#!/usr/bin/env python

# Made with the unfortunate belief that everything in Python should be iterable and dict-like

import boto.dynamodb
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError


class table(object):

	def __init__(self, dynamo_connection, table_name, item_class=None):
		self._iter_current_results = None
		self._iter_current_iter = None
		self._iter_batch_size = 20
		self.item_class = item_class
		self.cache = {}
		self.dynamo = dynamo_connection
		self.table_name = table_name
		self.table = self.dynamo.get_table(self.table_name)
		return

	def get(self, key):
		if key in self.cache:
			return self.cache[key]
		try:
			result = self.table.get_item(key.lower())
		except DynamoDBKeyNotFoundError:
			self.cache[key] = None
			return None
		self.cache[key] = self.item_class(self.table, item=result)
		return self.cache[key]

	def new(self, hash_key):
		# Should this be cached?
		result = self.get(hash_key)
		if result:
			raise IndexError
		return self.item_class(self.table, hash_key=hash_key)

	def next(self):
		""" Wrapper around full table scans ~ not sure how efficient this is """
		if not self._iter_current_results:
			self._iter_current_results = self.table.scan(max_results=self._iter_batch_size, exclusive_start_key=None)
			self._iter_current_iter = self._iter_current_results.__iter__()
		try:
			item = next(self._iter_current_iter)
		except StopIteration:
			if not self._iter_current_results.last_evaluated_key or self._iter_current_results.count < self._iter_batch_size:
				raise StopIteration
			else:
				self._iter_current_results = self.table.scan(max_results=self._iter_batch_size, exclusive_start_key=self._iter_current_results.last_evaluated_key)
				self._iter_current_iter = self._iter_current_results.__iter__()
				item = next(self._iter_current_iter)
		return self.item_class(self.table, item=item)

	def __iter__(self):
		return self

	def __len__(self):
		# Updated every 6 hours ... or so
		# return int(dynamodb_connection.describe_table(self.table_name)['Table']['ItemCount'])
		# Or slow/expensive and realtime
		return int(self.table.scan(count=True).count)

	def __getitem__(self, key):
		result = self.get(key)
		if result:
			return result
		else:
			raise IndexError

	def __contains__(self, key):
		result = self.get(key)
		if not result:
			return False
		return True

	def __delitem__(self, key):
		item = self.__getitem__(key)
		return item.delete()


class item(object):

	def __init__(self, table, item=None, hash_key=None):
		self.table = table
		self.modified = False
		if item:
			# Pre-existing entry
			self.item = item
		else:
			# New entry
			self.item = self.table.new_item(hash_key=hash_key)
		return

	def __setitem__(self, key, value):
		self.item[key] = value
		self.modified = True

	def __getitem__(self, key):
		return self.item[key]

	def write(self):
		if self.modified:
			self.item.put()

	def __str__(self):
		return "<Item[\"%s\"]>" % (self.__getitem__("id"))

if __name__ == "__main__":
	print "Go away."



