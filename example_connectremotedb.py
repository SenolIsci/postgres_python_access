# -*- coding: utf-8 -*-
"""
Created on Mon Apr 12 09:50:10 2021

@author: senol.isci
"""
import os
import psycopg2
from dotenv import load_dotenv  #to load database secrets stored in a file as environment variables

load_dotenv(".env.txt")  #to load database secrets stored in a file as environment variables

url=os.environ["DATABASE_URL"]

connection=psycopg2.connect(url)
cursor=connection.cursor()
cursor.execute("SELECT * FROM users;")
first_user=cursor.fetchone()

print(first_user)

connection.close()