# Databricks notebook source
# MAGIC %md ###Reading Data.Json

# COMMAND ----------

Df_Json = spark.read.format('json').option('header',True).option('multiline',False).option('inferschme',True)\
                    .load('/FileStore/tables/drivers.json')

# COMMAND ----------

Df_Json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Reading data

# COMMAND ----------

Df = spark.read.format("csv").option("header",True).option("infershema",True).load("/FileStore/tables/BigMart_Sales__1_.csv")

# COMMAND ----------

Df.display()

# COMMAND ----------

My_ddl_Schema = '''

 Item_Identifier string,
 Item_Weight double,
 Item_Fat_Content string,
 Item_Visibility double,
 Item_Type string, 
 Item_MRP double, 
 Outlet_Identifier string, 
 Outlet_Establishment_Year integer, 
 Outlet_Size string,
 Outlet_Location_Type string,
 Outlet_Type string,
 Item_Outlet_Sales double 

 '''

# COMMAND ----------

Df = spark.read.format("csv").option("header",True).schema(My_ddl_Schema).load("/FileStore/tables/BigMart_Sales__1_.csv")

# COMMAND ----------

Df.display()

# COMMAND ----------

Df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC ###Selelct Statement

# COMMAND ----------

Df.select(col('item_identifier'),col('Item_Weight'),col('Item_Fat_Content')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Alias

# COMMAND ----------

Df.select(col('Item_identifier').alias('item_id') ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Filter Condition

# COMMAND ----------

Df.filter((col('item_type') == 'Soft Drinks') & (col('item_weight') > 10)).display()

# COMMAND ----------

Df.filter((col('outlet_size').isNull()) & (col('outlet_location_type').isin('Tier 1','Tier 2') )).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Withcolumnrenamed

# COMMAND ----------

Df.withColumnRenamed('item_weight','Item_Wt').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Withcolumn

# COMMAND ----------

df=Df.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

Df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Withcolumn

# COMMAND ----------

Df = Df.withColumn('Multiply',col('item_weight')* col('item_mrp'))
Df.display()

# COMMAND ----------

Df.withColumn('item_fat_content',regexp_replace(col('item_fat_content'),'Regular','reg'))\
    .withColumn('item_fat_content',regexp_replace(col('item_fat_content'),'Low Fat','LF')).display()

# COMMAND ----------

Df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Type Cast

# COMMAND ----------

Df = Df.withColumn('item_weight',col('item_weight').cast('double'))
Df.display()

# COMMAND ----------

Df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Sort and Limit

# COMMAND ----------

Df.sort(['item_weight','item_visibility'],ascending=[1,0]).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Drop

# COMMAND ----------

Df.drop(col('item_weight'),col('item_visibility')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Drop Duplicates

# COMMAND ----------

Df.dropDuplicates(subset=['item_type']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Distinct
# MAGIC

# COMMAND ----------

Df.select(col('item_type')).distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create table

# COMMAND ----------

schema1 = ['ID','Name']
Data1 = [('1','Mani'),('2','Vidhuran')]
df1= spark.createDataFrame(Data1,schema1)


# COMMAND ----------

Schema2 = ['ID','Name']
Data2 = [('3','Priya'),
                    ('2','Vidhuran')
                         ]
df2= spark.createDataFrame(Data2,Schema2)

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Union

# COMMAND ----------

df1.union(df2).distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Union by name

# COMMAND ----------

Schema2 = ['Name','ID']
Data2 = [('Priya','3'),
                    ('Vidhuran','2')
                         ]
df2= spark.createDataFrame(Data2,Schema2)

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###String function

# COMMAND ----------

Df.select(col('Item_weight'),initcap(col('item_type'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Date Function

# COMMAND ----------

Df = Df.withColumn('Curr_date',current_date())

# COMMAND ----------

df = Df.withColumn('Currdate_7',date_add('curr_date',7))

# COMMAND ----------

Df.withColumn('currdate_-7',date_sub('curr_date',7)).display()

# COMMAND ----------

df.withColumn('datediffcol',datediff('currdate_7','curr_date')).display()   

# COMMAND ----------

df.withColumn('curr_date',date_format('curr_date','dd-MM-yyyy')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Handling Null Values

# COMMAND ----------

df.dropna(subset='item_weight').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Filling Null with value

# COMMAND ----------

df.fillna('Not Available',subset=['outlet_size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Split

# COMMAND ----------

df.withColumn('outlet_type',split('outlet_type',' ')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Indexing

# COMMAND ----------

df.withColumn('outlet_type',split('outlet_type',' ')[1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Explode

# COMMAND ----------

df_exp = df.withColumn('outlet_type',split('outlet_type',' '))


# COMMAND ----------

df_exp.withColumn('outlet_type',explode('outlet_type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Array contain

# COMMAND ----------


df_exp.withColumn('Flagtype',array_contains('outlet_type','Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Group by

# COMMAND ----------

df.groupBy('item_type','outlet_size').agg(sum('item_mrp'),avg('item_mrp')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Collect list

# COMMAND ----------

schema = ('User string,Book string')
data = [('User1','book1'),
        ('user1','book2'),
        ('user2','book2'),
        ('user2','book3'),
        ('user3','book1')
        ]
df_book = spark.createDataFrame(data, schema)
df_book.display()


# COMMAND ----------

df_book.groupBy('user').agg(collect_list('book')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Pivot

# COMMAND ----------

df.groupBy('item_type').pivot('outlet_size').agg(avg('item_mrp')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###When-otherwise

# COMMAND ----------

df= df.withColumn('item_category',when(col('item_type') == 'Meat','Nonveg').otherwise('Veg'))

# COMMAND ----------

df.withColumn('item_price_category',
              when(
    (col('item_category') == 'Veg') & (col('item_mrp')>100),'Expensive')
     .when((col('item_category')=='Veg') & (col('item_mrp')<=100),'Inexpensive')         
    .otherwise('nonveg_exp')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###InnerJoin

# COMMAND ----------

schemaj =('emp_id string,emp_name string,dept_id string')
dataj = [
        ('1','Mani','d01'),
        ('2','Dinesh','d02'),
        ('3','sharuk','d03'),
        ('4','Bilagi','d03'),
        ('5','Srikanth','d04')

]
df_emp1 = spark.createDataFrame(dataj,schemaj)


# COMMAND ----------

df_emp1.display()

# COMMAND ----------

schemaj2 = ('dept_id string, department string')
dataj2 = [

    ('d01','HR'),
    ('d02','software developer'),
    ('d03','Service'),
    ('d04','Manitenance'),
    ('d05','Marketing')
]
df_emp2 = spark.createDataFrame(dataj2,schemaj2)


# COMMAND ----------

df_emp2.display()

# COMMAND ----------

df_emp1.join(df_emp2,df_emp1['dept_id'] == df_emp2['dept_id'],'inner').display()

# COMMAND ----------

df_emp1.join(df_emp2,df_emp1['dept_id']==df_emp2['dept_id'],'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Rownumber

# COMMAND ----------

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# COMMAND ----------

df.withColumn('rownum',row_number().over(Window.orderBy(desc('item_identifier')))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Rank

# COMMAND ----------

df.withColumn('denserankcol',dense_rank().over(Window.orderBy(col('item_identifier').desc()))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Cumulative sum

# COMMAND ----------

df.withColumn('cumsum',sum('item_mrp').over(Window.orderBy(col('item_type').desc()).rowsBetween(Window.unboundedPreceding,Window.currentRow ))).display()

# COMMAND ----------

df.withColumn('totalsum',sum('item_mrp').over(Window.orderBy(col('item_type').desc()).rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing ))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Writing Mode

# COMMAND ----------

# MAGIC %md
# MAGIC ####Append

# COMMAND ----------

df.write.format('csv').mode('append')\
.option('path','/FileStore/tables/CSV/BigMart_Sales__1_.csv').save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Overwrite

# COMMAND ----------

df.write.format('csv').mode('overwrite').option\
    ('path','/FileStore/tables/CSV/BigMart_Sales__1_.csv').save()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Error

# COMMAND ----------

df.write.format('csv').mode('error').option\
    ('path','/FileStore/tables/CSV/BigMart_Sales__1_.csv').save()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Ignore

# COMMAND ----------

df.write.format('csv').mode('ignore').option\
    ('path','/FileStore/tables/CSV/BigMart_Sales__1_.csv').save()

# COMMAND ----------

