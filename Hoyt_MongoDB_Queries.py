# -*- coding: utf-8 -*-
"""
Created on Sat Jul 22 21:26:37 2023

@author: Lowhorn
"""

#import pymongo
import pymongo

"""
Exercise 1
Create a mongo_db connection with pymongo to your database
https://pymongo.readthedocs.io/en/stable/examples/authentication.html

For the homework we will be using the sample_restaurants.restaurants collection. 

Using find(), write a find query to extract the Italian restaurants in Manhattan to a Python list. 
Use len() to count the number of restaurants located in Manhattan. 

***Note*** All MongoDB functions and fields MUST be in quotes inside of the find() method. Ex $and should be "$and".

https://www.w3schools.com/python/python_mongodb_find.asp
"""
#making connection 
myclient = pymongo.MongoClient("mongodb+srv://hoytm:fYej1ykh1cL8G15H@cluster0.xtrafrh.mongodb.net/?retryWrites=true&w=majority")
mydb = myclient["sample_restaurants"]
mycol = mydb["restaurants"]

result = mycol.find({ "cuisine": "Italian", "borough": "Manhattan"})

output = []
for r in result:
    output.append(r)
len(output)

"""
Exercise 2
Using find, determine how many Japanese and Italian restaurants have an A rating in Queens.

"""
query = {"$and":[{"$or":[{'cuisine': 'Italian'}, 
                         {'cuisine': 'Japanese'}]},
                 {'borough': 'Queens'}, 
                 {'grades':{"$elemMatch": {'grade': 'A'}}}]}


result2 = mycol.find(query)

output2 = []
for x in result2:
    output2.append(x)
len(output2)  



"""
Exercise 3
The following MongoDB aggregation query is missing a aggregation expression that will calculate the BSON size of the documents. 
A list of these can be found at the end of this week's notes. Identify the missing aggregation expression.
Print the 10 document ids and sizes that have the highest BSON size. 
"""

res = mycol.aggregate([
    { "$addFields": {
        "bsonsize": { "$bsonSize": "$$ROOT" }
    }},
    { "$sort": { "bsonsize": -1 }},
    { "$limit": 10 },
    { "$project": {
        "_id": 1,
        "bsonsize": 1
    }}
])
    
topten = []

for a in res:
    topten.append(a)

print(topten)
    
"""
Exercise 4
Find all of the restaurants that have NOT had an 'A', 'B', and 'Not Yet Graded' rating. How many restaurants is this?
"""
query2 = {"$nor": [{'grades':{"$elemMatch":{"grade":"A"}}}, 
                 {'grades':{"$elemMatch":{"grade":"B"}}}, 
                 {'grades':{"$elemMatch":{"grade":"Not Yet Graded"}}}]} #How do i get rid of these values?

result3 = mycol.find(query2)

output3 = []
for y in result3:
    output3.append(y)
len(output3)  