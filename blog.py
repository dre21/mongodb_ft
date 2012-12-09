import pymongo
import logging
import datetime

from indexer import Indexer
from bson.objectid import ObjectId


class Blog(object):

	
	def __init__(self):
		
		# create indexer 
		self.Idx = Indexer()

		# create two connection instances
		self.Post = None
		self.InvIdx = None

		self.index_fields = []

	def set_db(self,Blog_DB):
		self.Post = Blog_DB.posts
		self.InvIdx = Blog_DB.invidx

	def set_index_fields(self,fields):

		if not isinstance(fields, list): 
			raise Exception("Fields must be a list")

		self.index_fields = fields
		self.Idx.set_idx_fields(fields)


	def save_post(self,post):
		logging.debug('save_post: ' + str(post))
		if self.index_fields == []:
			raise Exception("No fields to index. Please set it first!")
		if isinstance(post,list): 
			raise Exception("Only accept 1 post")

		
		if logging.root.level == logging.DEBUG:
			post_start_time = datetime.datetime.utcnow()

		# inserting post to posts collection
		obj_id = self.Post.insert(post)

		if logging.root.level == logging.DEBUG:
			post_end_time = datetime.datetime.utcnow()

		
		if obj_id == None:
			raise Exception("Error saving to mongodb")
		logging.debug('Saving post to mongo is OK')

		# strip unnecessary string
		#obj_id_strip = str(obj_id).strip('ObjectId("').rstrip('")')
		#logging.debug('strip object_id to: ' + obj_id_strip)
		
		if logging.root.level == logging.DEBUG:
			idx_start_time = datetime.datetime.utcnow()

		# get word
		words = self.Idx.index(post)		

		# updating words to inverted index
		# using loop
		# TODO: change to bulk update
		for word in words:
			#print word
			#self.InvIdx.update({"word":word},{"$push":{"docs":obj_id_strip}},True)
			self.InvIdx.update({"word":word},{"$push":{"docs":obj_id}},True)
		
		if logging.root.level == logging.DEBUG:
			idx_end_time = datetime.datetime.utcnow()
			# print info
			post_time = post_end_time-post_start_time
			idx_time = idx_end_time-idx_start_time
			total_time = post_time + idx_time
			
			logging.debug('time to save post: ' +str(post_time.total_seconds()))
			logging.debug('time to save idx: ' +str(idx_time.total_seconds()))
			logging.debug('total time: ' +str(total_time.total_seconds()))

		return obj_id
	
	def get_dummy_post(self,number):
		
		if (number<0) or (number>4): 
				raise Exception("Choose 1..4")

		posts = {}
		posts[1] = "Six people have been shot dead after a Russian lawyer opened fire on his colleagues at a pharmacy company"
		posts[2] = "Water and Venice usually go together like bees and honey. But not when there's as much rain"
		posts[3] = "Two men inside the utility truck have a lucky escape after a passing freight train collides with their vehicle"
		posts[4] = "Super storm Sandy gives New York a historic drenching.\nBattery Park in lower Manhattan floods as record high water"

		return {"title":"Dummy post "+str(number) ,"content": posts[number], "time":str(datetime.datetime.utcnow())}

	def clear(self):
		self.Post.remove()
		self.InvIdx.remove()

	def search(self,input_text):


		# get time: start first query
		if logging.root.level == logging.DEBUG:
			query_idx_start_time = datetime.datetime.utcnow()

		# tokenize query
		words_text_input = self.Idx.tokenize(text_input)

		# build query to get doc_ids
		list_words_text_input = []
		for word_text_input in words_text_input:
			#print word_text_input
			cond_words_text_input = {"word": word_text_input}
			list_words_text_input.append(cond_words_text_input)
		final_words_text_input = {"$or":list_words_text_input}
		
		# get doc_ids from inverted index
		doc_ids = [queryIdx.values()[0] for queryIdx in self.InvIdx.find( final_words_text_input, {"docs" :1 })]
		# remove duplicate doc_id
		doc_ids = set([doc_id[0] for doc_id in doc_ids])

		# get time: end first query & start second query
		if logging.root.level == logging.DEBUG:
			query_idx_end_time = datetime.datetime.utcnow()
			query_col_start_time = query_idx_end_time

		# build query to get documents by doc_ids
		list_doc = []
		for doc_id in doc_ids:
			cond_doc = {"_id": ObjectId(doc_id)}
			list_doc.append(cond_doc)
		final_doc = {"$or":list_doc}

		# get post from posts collection
		docs = self.Post.find(final_doc)


		if logging.root.level == logging.DEBUG:
			query_col_end_time = datetime.datetime.utcnow()
			
			# print info
			
			query_idx_time = query_idx_end_time - query_idx_start_time
			query_col_time = query_col_end_time - query_col_start_time
			total_time = query_idx_time + query_col_time
						
			logging.debug('time to query invidx: ' +str(query_idx_time.total_seconds()))
			logging.debug('time to query posts: ' +str(query_col_time.total_seconds()))
			logging.debug('total query time: ' +str(total_time.total_seconds()))

		return docs
		
	# end of function
# end of class



if __name__ == '__main__':

	logging.root
	logging.root.setLevel(logging.DEBUG)
	
	logging.debug('Create Connection')
	Con = pymongo.Connection('localhost')
	# set db name: myblog
	Blog_DB = Con.myblog

	logging.debug('Create Blog')
	Blog = Blog()
	# set database for blog
	Blog.set_db(Blog_DB)
	# set fields to be indexed
	Blog.set_index_fields(['title','content'])

	# =============================
	#	Test inserting document
	# =============================
	# clear db
	Blog.clear()
	# inserting some posts for testing
	logging.debug("insert post1 to db")	
	post = Blog.get_dummy_post(1)
	obj_id = Blog.save_post(post)
	
	logging.debug("insert post2 to db")
	post = Blog.get_dummy_post(2)
	obj_id = Blog.save_post(post)
	
	logging.debug("insert post3 to db")
	post = Blog.get_dummy_post(3)
	obj_id = Blog.save_post(post)

	# =============================
	#	Test querying document
	# =============================
	#input text
	print "\n## MongoDB Real Time Full Text Search - Python Driver ##\n"
	text_input = raw_input('Input Full Text Search : ')

	docs = Blog.search(text_input)
	for doc in docs:
		print doc







