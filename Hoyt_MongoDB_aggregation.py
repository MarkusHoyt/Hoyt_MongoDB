# -*- coding: utf-8 -*-
"""
Created on Sat Jul 22 21:26:37 2023

@author: Mark Hoyt
"""

#import pymongo
import pymongo



"""
Exercise 1
Create a mongo_db connection with pymongo to your database
https://pymongo.readthedocs.io/en/stable/examples/authentication.html

For the homework we will be using the sample_mflix.movies collection. 

What is the title of the movie with the highest IMDB rating?

***Note*** match, sort, limit, project.
collection.aggregate(query) is the syntax for aggregation pipelines in Python. 

https://pymongo.readthedocs.io/en/stable/examples/aggregation.html
"""
myclient = pymongo.MongoClient("mongodb+srv://hoytm:password@cluster0.xtrafrh.mongodb.net/?retryWrites=true&w=majority")
mydb = myclient["sample_mflix"]
mycol = mydb["movies"]

query = [
    {
        '$match': {
            'type': 'movie', 
            'imdb.rating': {
                '$gt': 9
            }
        }
    }, {
        '$sort': {
            'imdb.rating': -1
        }
    }, {
        '$limit': 1
    }, {
        '$project': {
            'title': 1, 
            'imdb.rating': 1
        }
    }
]
        
res = mycol.aggregate(query)

out = []
for r in res:
    out.append(r)
    print(r)
"""
Exercise 2
Which year had the most titles released? 
***Note*** group, sort, limit

"""

query2 = [
    {
        '$group': {
            '_id': '$year', 
            'count': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            'count': -1
        }
    }, {
        '$limit': 1
    }
]
        
res2 = mycol.aggregate(query2)

out = []
for r in res2:
    out.append(r)
    print(r)
  
"""
Exercise 3
What are the four directors with the most titles accredited to them? 
***Note*** project, unwind, group, sort, limit

"""

query3 = [
    {
        '$project': {
            'directors': 1, 
            'title': 1
        }
    }, {
        '$unwind': {
            'path': '$directors'
        }
    }, {
        '$group': {
            '_id': '$directors', 
            'count': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            'count': -1
        }
    }, {
        '$limit': 4
    }
]
        
res3 = mycol.aggregate(query3)

out = []
for r in res3:
    out.append(r)
    print(r)
  
"""
Exercise 4
Show the title and number of languages the movie was produced in for the following: Year:2013, genre:'Action'

Movie with the most langguages
"""

query4 = [
    {
        '$match': {
            'type': 'movie', 
            'year': 2013, 
            'genres': 'Action'
        }
    }, {
        '$unwind': {
            'path': '$languages'
        }
    }, {
        '$group': {
            '_id': '$title', 
            'count': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            'count': -1
        }
    }, {
        '$limit': 5
    }
]

res4 = mycol.aggregate(query4)

out = []
for r in res4:
    out.append(r)
    print(r)
