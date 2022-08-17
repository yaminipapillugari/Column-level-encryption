# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.nabudatabricksstorage.dfs.core.windows.net",
    dbutils.secrets.get(scope="sample_scope",key="sample_key"))

# COMMAND ----------

pip install cryptography

# COMMAND ----------

# MAGIC %md
# MAGIC ##Creation of fernet key

# COMMAND ----------

from cryptography.fernet import Fernet
generated_key = Fernet.generate_key()
print(generated_key)
f=Fernet(generated_key)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Reading csv file from adls2

# COMMAND ----------

df = spark.read.option("header",True).csv("abfss://sample@nabudatabricksstorage.dfs.core.windows.net/Revenue_data.csv")

# COMMAND ----------

df.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Defining udf

# COMMAND ----------

def encrypt_col_val(data):
    encrypt_val_b=bytes(data, 'utf-8')
    encrypt_val = f.encrypt(encrypt_val_b)
    encrypt_val = str(encrypt_val.decode('ascii'))
    return encrypt_val

# COMMAND ----------

# MAGIC %md
# MAGIC ##Creation of udf and using udf in a dataframe to encrypt the column

# COMMAND ----------


from pyspark.sql.functions import udf, lit
 
# Register UDF's
encrypt = udf(encrypt_col_val)

encrypted_df = df.withColumn("encrypt_password", encrypt("Password")).drop("Password")
display(encrypted_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Writing encrypted csv file to adls2

# COMMAND ----------

encrypted_df.coalesce(1).write.mode("overwrite").option("header","true").csv("abfss://sample@nabudatabricksstorage.dfs.core.windows.net/Col_encrypt_revenue_data.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #Decryption of column data

# COMMAND ----------

#Reading encryted data from the adls2
df1 = spark.read.option("header","true").csv("abfss://sample@nabudatabricksstorage.dfs.core.windows.net/Col_encrypt_revenue_data.csv")
df1.show(truncate=False)

# COMMAND ----------

#Defining user defined function
def decrypt_col_val(data):
    decrypt_val = f.decrypt(data.encode()).decode()
    return decrypt_val

# COMMAND ----------

decrypt = udf(decrypt_col_val)

# This is how you can decrypt in a dataframe
 
decrypted_df = df1.withColumn("decrypt_password", decrypt("encrypt_password"))
display(decrypted_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Referred link
# MAGIC #####https://www.databricks.com/notebooks/enforcing-column-level-encryption.html