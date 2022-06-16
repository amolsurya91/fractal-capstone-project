#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system('pip install faker ')

from faker import Faker
fake = Faker('en_IN')


# In[22]:


import random


# In[23]:



fs=['Maharashtra','Punjab','Madhya Pradesh','Rajasthan']
fc=['Pune','Nagpur','Amravati','Thane','Nashik'
    ,'Ludhiana','Amritsar','Jalandhar','Patiala','Bathinda'
   ,'Indore','Bhopal','Jabalpur','Gwalior','Katni',
   'Jaipur','Jodhpur','Kota','Bhiwadi','Bikaner']


# In[24]:


# function for customer table
def gen_customer(i):
    return { 
    'customerid':i,   
    'name': fake.unique.name(),
    'address': fake.street_address(),
    'state': random.choice(fs),
    'city': random.choice(fc),
    'pincode': fake.postcode(),
    'update_timestamp': fake.date_time_between(start_date="-3y", end_date="now")
    }

customers = [gen_customer(i+1) for i in range(1000)]


# In[25]:



#function for product table

def gen_product(i):
    return {
    'productid':i,   
    'productcode': fake.unique.bothify(text='?##'),
    'productname': fake.unique.bothify(text='Product???###'),
    'sku': fake.unique.bothify(text='#??'),
    'rate': random.randrange(50,5000,50),
    'isactive': random.choices(['True','False'])
    }

products = [gen_product(i+1) for i in range(100)]


# In[34]:


#function for order details table
def gen_orderdetails(i):
    return {
    'orderdetailsid':i,    
    'orderid': random.randrange(1,2000),
    'customerid': random.randrange(1,1000),
    'orderupdatetimestamp': fake.date_time_between(start_date="-3y", end_date="now"),
    'orerstatus': random.choices(['Received','Inprogress','Delivered'])
    }

orderdetails = [gen_orderdetails(i) for i in range(6000)]


# In[28]:


#function for order items
def gen_orderitems(i):
    return {
    'order_itemsid':i,   
    'orderid': random.randrange(1,20000),
    'productid': random.randrange(1,100),
    'quantity': random.randrange(1,10)         
    }
orderitems = [gen_orderitems(i) for i in range(40000)]


# In[29]:



import pandas as pd
dfcustomer = pd.DataFrame(customers)
dfproduct = pd.DataFrame(products)
dforderdetails = pd.DataFrame(orderdetails)
dforderitems = pd.DataFrame(orderitems)


# In[27]:


get_ipython().system('pip install psycopg2')


# In[30]:


import psycopg2


# In[31]:


src_conn = psycopg2.connect(database='postgres',user='postgres',password='root',host='130.211.121.52',port='5432')


# In[32]:


con=src_conn.cursor()


# In[35]:


source1 = pd.read_sql_query('select * from orderdetails cm;', src_conn)
source1


# In[18]:


from sqlalchemy import create_engine
import os


# In[19]:


#engine = create_engine(f'postgresql://{'Postgres'}:{'root'}@{"130.211.121.52"}:5432/postgres')
engine = create_engine('postgresql://postgres:root@130.211.121.52:5432/postgres')


# In[20]:


dfcustomer.to_sql('customer_master', engine, if_exists='append', index=False)


# In[37]:


dfproduct.to_sql('product_master', engine, if_exists='append', index=False)


# In[33]:


dforderdetailsd.to_sql('orderdetails', engine, if_exists='append', index=False)


# In[36]:


dfoideritems.to_sql('order_items', engine, if_exists='append', index=False)


# In[ ]:




