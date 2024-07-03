# %%
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType
from pyspark.sql import functions as F


# %%


# %%

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "5c975aff-8753-41ef-9aef-deb3a6b0ede3",
"fs.azure.account.oauth2.client.secret": '',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/60aaf3c6-3a32-43e6-a365-065b0dbae959/oauth2/token"}

dbutils.fs.mount(
source = "abfss://tokyo-olympic-data@tokyoolympicdatashehroz.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolymic",
extra_configs = configs)  

# %%
%fs
ls "/mnt/tokyoolymic"

# %%
spark

# %%
#Infer Schema set to true so Spark can automatically set Data Types for columns correctly. 

# %%
athletes = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-data/athletes.csv")
coaches = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-data/coaches.csv")
entriesgender = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-data/entriesgender.csv")
medals = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-data/medals.csv")
teams = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-data/teams.csv")

# %%
athletes.show(5)

# %%
athletes.printSchema()


# %%
coaches.show(4)


# %%
coaches.printSchema()


# %%
entriesgender.show()


# %%
entriesgender.printSchema()


# %%
medals.show()

# %%
medals.printSchema()


# %%
teams.show()


# %%
teams.printSchema()


# %%
# Aggregating medals information by country
country_medals = medals.groupBy("Team_Country").agg(
    F.sum("Gold").alias("Total_Gold"),
    F.sum("Silver").alias("Total_Silver"),
    F.sum("Bronze").alias("Total_Bronze"),
    F.sum("Total").alias("Total_Medals")
).orderBy("Total_Medals", ascending=False)
country_medals.show()

# %%
# Correcting the join by resolving ambiguous column references using aliases
athletes_coaches = athletes.join(
    coaches,
    (athletes["Country"] == coaches["Country"]) & (athletes["Discipline"] == coaches["Discipline"]),
    "left_outer"
).select(
    athletes["PersonName"],
    athletes["Country"].alias("AthleteCountry"),
    athletes["Discipline"].alias("AthleteDiscipline"),
    coaches["Name"].alias("CoachName"),
    coaches["Event"]
)

# Now display the results
athletes_coaches.show()


# %%
top_gold_medal_countries = medals.orderBy("Gold", ascending=False).select("Team_Country","Gold").show()


# %%

# Calculate the average number of entries by gender for each discipline
average_entries_by_gender = entriesgender.withColumn(
    'Avg_Female', entriesgender['Female'] / entriesgender['Total']
).withColumn(
    'Avg_Male', entriesgender['Male'] / entriesgender['Total']
)
average_entries_by_gender.show()
     

# %%
# Writing country_medals DataFrame
country_medals.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolymic/transformed-data/country_medals")

# Writing athletes_coaches DataFrame
athletes_coaches.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolymic/transformed-data/athletes_coaches")

# Writing average_entries_by_gender DataFrame
average_entries_by_gender.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolymic/transformed-data/average_entries_by_gender")


# %%
athletes.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformed-data/athletes")


# %%


coaches.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/coaches")
entriesgender.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/entriesgender")
medals.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/medals")
teams.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/teams")

