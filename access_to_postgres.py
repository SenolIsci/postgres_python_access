# -*- coding: utf-8 -*-
"""
Created on Mon Apr 12 15:21:12 2021

@author: senol.isci
"""
import logging

import psycopg2

import os

from dotenv import load_dotenv  #to load database secrets stored in a file as environment variables

import functools

#template for building more complex decorators

from contextlib import contextmanager


            
class PostgresAccess():
    def __init__(self, dbpath):
        self.dbpath = dbpath
        self.connection = None
        self.cursor=None
        logging.basicConfig(format='%(asctime)s: %(levelname)s: %(funcName)s:%(lineno)d: %(message)s',
                            level=logging.INFO, handlers=[logging.FileHandler("program_log.log"), logging.StreamHandler()])


    def __repr__(self):
        return f"Database access: \n Database: {self.dbpath}\n conncetion status:{self.connection}\n cursor.status: {self.cursor}"
        

    def open_connection(self):
        if self.connection == None:
            # open and if not exxst create and open
            try:
                self.connection = psycopg2.connect(self.dbpath)
                self.cursor=self.connection.cursor()
                #foreign_keys = ON is necessary to do necessary action after deleting/updating a row containing a referenced foreign key. 
                #self.execute_sql("PRAGMA foreign_keys = ON")
                
                logging.info(f"connection establisted to {self.dbpath}")
            except psycopg2.Error:
                logging.error(f"connection failure to {self.dbpath}")


    def execute_sql(self, sql_command: str, fields=None):
        if self.connection != None:
            """this method prevents sql injection for sqlite"""
            try:
                #create a cursor, a transaction and commit due to with context manager   
             if fields:
                self.cursor.execute(sql_command,fields)
                self.connection.commit()
             else:
                self.cursor.execute(sql_command)
                self.connection.commit()
                logging.info(f"execution sucessfull: {sql_command}")
            except psycopg2.Error:
                logging.error(f"execution failure: {sql_command}")
    
            return self.cursor
        else:
            return []

    def close_connection(self):
        if self.connection != None:
            try:
                self.connection.close()
                self.connection=None
                self.cursor=None
                logging.info(f"close connection.")
            except psycopg2.Error:
                logging.warning(f"close connection.")

    #create a connection managger
    @contextmanager
    def session(self):

        try:
            yield self.open_connection()
        finally:
            self.close_connection()

if __name__=="__main__":
    
    
    load_dotenv(".env.txt")  #to load database secrets stored in a file as environment variables

    url=os.environ["DATABASE_URL"]

    command1="""CREATE TABLE IF NOT EXISTS entries(content TEXT, date TEXT);"""
    command2 = """CREATE TABLE IF NOT EXISTS users(
        id SERIAL PRIMARY KEY,
        first_name TEXT, 
        surname TEXT, 
        age INTEGER
        );"""
    command3= "INSERT INTO users (first_name, surname,age) VALUES ('Rolf','Smith',35);" 
    command4 = "INSERT INTO users (first_name, surname,age) VALUES ('John','Snow','19');" 

    #secure command against sql injection:
    command5="INSERT INTO users  (first_name, surname,age) VALUES (%s,%s,%s);" #NULL for primary key field.
    command5_tuple=('Bla','Blo','55')

    command6="SELECT * FROM users;"
    command7="SELECT * FROM users WHERE age > 19;"
    command8 = "SELECT * FROM users WHERE first_name ='John';"
    command9 = "SELECT * FROM users WHERE first_name !='John';"
    command10 = "SELECT * FROM users WHERE first_name !='John' AND age > 19;"

    #secure command against sql injection:
    command11 = "SELECT * FROM users WHERE first_name = %s;"
    command11_tuple = ('John',)

    command12="DROP TABLE IF EXISTS users;"
    
    
    
    command00="""
            CREATE TABLE IF NOT EXISTS supplier_groups (
                group_id SERIAL PRIMARY KEY,
                group_name text NOT NULL
            );
            """
    
    command01 = """
                CREATE TABLE IF NOT EXISTS suppliers (
                supplier_id   SERIAL PRIMARY KEY,
                supplier_name TEXT    NOT NULL,
                group_id    INTEGER,
                FOREIGN KEY (group_id)
                REFERENCES supplier_groups (group_id) 
                    ON UPDATE SET NULL
                    ON DELETE SET NULL
            );
            """


    command02="""

            INSERT INTO supplier_groups (group_name)
            VALUES
            ('Domestic'),
            ('Global'),
            ('One-Time');
            """
    command03="INSERT INTO suppliers (supplier_name, group_id) VALUES ('HP', 2);"
    command04="INSERT INTO suppliers (supplier_name, group_id) VALUES('XYZ Corp', 2);"
    command05="INSERT INTO suppliers (supplier_name, group_id) VALUES('ABC Corp', 3);"


    #demonstration to show that when group_id = 3 record in suplier_groups deleted group_id field of supplier is automatically set to null because of  "ON DELETE SET NULL" in table creation

    command000 = "DELETE FROM supplier_groups WHERE group_id = 3;"
    commandjoin ="""
            SELECT supplier_name,supplier_groups.group_name 
            FROM suppliers
            INNER JOIN supplier_groups
            ON suppliers.group_id = supplier_groups.group_id;
            """
    #view is a query object stored in the database and can be used later on.
    commandcreateview = """
            CREATE OR REPLACE VIEW suplquery
            AS 
            SELECT supplier_name,supplier_groups.group_name 
            FROM suppliers
            INNER JOIN supplier_groups
            ON suppliers.group_id = supplier_groups.group_id;
            """
    commandcallview = "SELECT * FROM suplquery;"

    commandwith="""
    WITH cte_count_users_by_groups AS (
        SELECT group_id,
            COUNT(supplier_id) user_count
        FROM   suppliers
        GROUP  BY group_id
    )
    SELECT supplier_groups.group_id,
           group_name,
           user_count
    FROM supplier_groups
        LEFT JOIN cte_count_users_by_groups USING (group_id); 
        """

    create_sequence="CREATE SEQUENCE IF NOT EXISTS my_sequence START 101;"
    command_callnextsequence="SELECT nextval('my_sequence');"

    sq = PostgresAccess(url)
    sq2 = PostgresAccess(url)
    
    with sq.session():

        sq.execute_sql(command1)
        sq.execute_sql(command2)
    
        sq.execute_sql(command3)
        sq.execute_sql(command4)
        sq.execute_sql(command5, command5_tuple)
        rows=sq.execute_sql(command10)
        for row in rows:
            print(row)
        rows = sq.execute_sql(command11, command11_tuple)
        for row in rows:
            print(row)
        sq.execute_sql(command00)
        sq.execute_sql(command01)
        sq.execute_sql(command02)
        sq.execute_sql(command03)
        sq.execute_sql(command04)
        sq.execute_sql(command05)


    with sq.session():
        rows = sq.execute_sql(commandjoin)
        for row in rows:
            print(row)
  
    with sq.session():
        sq.execute_sql(command000)


    with sq.session():
        sq.execute_sql(commandcreateview)
        rows=sq.execute_sql(commandcallview)
        for row in rows:
            print(row)        
    
    with sq.session():
        sq.execute_sql(create_sequence)
    
        rows=sq.execute_sql(command_callnextsequence)
        for row in rows:
            print(row)

    
    with sq2.session():
        rows=sq2.execute_sql(commandwith)
        for row in rows:
            print(row)
   

    logging.shutdown() #release logging handlers

