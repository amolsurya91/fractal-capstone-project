#!/usr/bin/env python
# coding: utf-8

# In[2]:


import psycopg2


# In[8]:


get_ipython().system(' pip install pandas-gbq -U')


# In[5]:


get_ipython().system('pip install psycopg2')


# In[13]:


import psycopg2


# In[14]:


import pandas as pd


# In[15]:


from pandas.io import  gbq


# In[74]:


src_conn = psycopg2.connect(database='postgres',user='postgres',password='root',host='130.211.121.52',port='5432')


# In[75]:


cur=src_conn.cursor()


# In[57]:


# dim address table
add_table = pd.read_sql_query('select row_number () over(order by customerid) addressid,address ,city ,state ,pincode '+
                           'from customer_master cm;', src_conn)


# In[ ]:


#=======================================================


# In[58]:


from google.oauth2 import service_account
from google.cloud import bigquery
import pandas_gbq


# In[59]:


credentials = service_account.Credentials.from_service_account_file("C:/Users/amol.suryawanshi/Downloads/key.json")
#credentials = service_account.Credentials.from_service_account_file(" /home/airflow/gcs/dags/scripts/key.json ")


# In[60]:


#dimAddress
add_table.to_gbq(destination_table='Sales_datasest.dim_address',project_id='lab-4-352804',credentials=credentials,if_exists='append')


# In[66]:


from sqlalchemy import create_engine
import os


# In[72]:


engine = create_engine('postgresql://postgres:root@130.211.121.52:5432/postgres')


# In[68]:


#staging address table 
add_table.to_sql('Staging_Address_table', engine, if_exists='replace', index=False)


# In[90]:


#dim customer table

dimCusomer_table = pd.read_sql_query('''select customerid ,name,sa.addressid 
                                ,update_timestamp start_date,cast('1991-09-22 18:04:52.000' as timestamp) end_date 
                                from customer_master cm join "Staging_Address_table" sa on cm.address =sa.address 
                                and cm.state=sa.state and cm.city=sa.city and cm.pincode =sa.pincode''', src_conn)


# In[92]:


dimCusomer_table.to_gbq(destination_table='Sales_datasest.dim_customer',project_id='lab-4-352804',credentials=credentials,if_exists='append')


# In[97]:


#dim_product
dim_product = pd.read_sql_query('''select productid, productcode,productname,sku,rate,isaccitve
,case when orderstatus=''Recieved'' then order_stastu_update_timestamp end start_date
,case when orderstatus=''Delivered'' then order_stastu_update_timestamp end End_date
from product_master pm  
join order_items oi on oi.prodictid=pm.productid
join orderdetails od on od.orderid=oi.orderid ''')


# In[98]:


dim_product.to_gbq(destination_table='Sales_datasest.dim_product',project_id='lab-4-352804',credentials=credentials,if_exists='append')


# In[93]:


#dim_order
dim_order = pd.read_sql_query('''select orderid ,order_status_update_timestamp ,order_status  
                                from orderdetails o ''', src_conn)


# In[94]:


dim_order.to_gbq(destination_table='Sales_datasest.dim_order',project_id='lab-4-352804',credentials=credentials,if_exists='append')


# In[96]:


#fact order details
fact_orderdetails = pd.read_sql_query('''select oi.orderid,order_status_update_timestamp as order_ststus_delivered_timestamp,productid,quantity
                                        from order_items oi 
                                        join orderdetails od on od.orderid=oi.orderid 
                                        where order_status="Deliverd"''',src_conn)


# In[99]:


fact_orderdetails.to_gbq(destination_table='Sales_datasest.fact_orderdetails',project_id='lab-4-352804',credentials=credentials,if_exists='append')


# In[100]:


#fact customer table
fact_daily_orders = pd.read_sql_query('''select cm.customerid ,oi.orderid,
case when order_status="Recieved" then order_status_updat_timestamp end as order_recieved_timestamp
case when order_status="Deliverd" then order_status_updat_timestamp end as order_Deliverd_timestamp,
,pincode,null orderamount,count(Productid) as itemCount
from customer_master cm 
join orderdetails od on cm.customerid =od.customerid 
join order_items oi on oi.orderid=od.orderid
where order_status="Deliverd"
group by 
cm.customerid ,oi.orderid,
case when order_status="Recieved" then order_status_updat_timestamp end as order_recieved_timestamp
case when order_status="Deliverd" then order_status_updat_timestamp end as order_Deliverd_timestamp,
,pincode,null orderamount''',src_conn)


# In[101]:


fact_daily_orders.to_gbq(destination_table='Sales_datasest.fact_daily_orders',project_id='lab-4-352804',credentials=credentials,if_exists='append')


# In[ ]:




