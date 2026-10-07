# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2aa11149-bbaf-4bad-9f98-04ec786dc851",
# META       "default_lakehouse_name": "lkh_fabric_enablement_migration",
# META       "default_lakehouse_workspace_id": "5393e23b-9032-471f-852f-52467f2d8a12",
# META       "known_lakehouses": [
# META         {
# META           "id": "2aa11149-bbaf-4bad-9f98-04ec786dc851"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Crear schema silver_p (si no existe) y tabla filtrada de capacity_all_accounts_usage
from pyspark.sql.functions import col

# 1) Asegurarnos de que exista el schema silver_p
spark.sql("CREATE SCHEMA IF NOT EXISTS silver_p")

# 2) Cargar la tabla bronze
df_capacity = spark.table("capacity_all_accounts_usage")

# 3) Filtrar solo registros donde Current_SKU sea un SKU tipo P (P, P1, P2, P3...)
#    Nota: se considera que cualquier valor que empiece por 'P' es válido

df_capacity_p = df_capacity.filter(col("Current_SKU").startswith("P"))

# 4) Guardar como tabla en el schema silver_p
#    Si quieres sobrescribir cada vez, usamos mode("overwrite")

df_capacity_p.write.mode("overwrite").saveAsTable("silver_p.capacity_all_accounts_usage_p")

print("Tabla silver_p.capacity_all_accounts_usage_p creada/actualizada correctamente.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lkh_fabric_enablement_migration.silver_p.capacity_all_accounts_usage_p LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Tabla agregada a nivel de cliente (Account_TPID / TPID)
from pyspark.sql.functions import col, struct, collect_list, sum as spark_sum

# Partimos de la tabla silver_p ya filtrada a SKUs tipo P
base_df = spark.table("silver_p.capacity_all_accounts_usage_p")

agg_df = (
    base_df
    .groupBy(
        col("Account_TPID").alias("Account_TPID"),
        col("TPID").alias("TPID")
    )
    .agg(
        # Arreglo con el detalle de cada capacidad y su uso
        collect_list(
            struct(
                col("Current_SKU").alias("Current_SKU"),
                col("Capacity_Id").alias("Capacity_Id"),
                col("CU_Hours_14d_").alias("CU_Hours_14d")
            )
        ).alias("sku_capacity_usage"),
        # Métrica resumida opcional: horas totales de consumo en 14 días
        spark_sum(col("CU_Hours_14d_")).alias("total_CU_Hours_14d")
    )
)

# Guardamos la tabla agregada en el mismo schema silver_p
agg_df.write.mode("overwrite").saveAsTable("silver_p.capacity_accounts_usage_agg")

print("Tabla silver_p.capacity_accounts_usage_agg creada/actualizada correctamente.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

df_agg = spark.sql("SELECT * FROM silver_p.capacity_accounts_usage_agg LIMIT 100")
display(df_agg)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Crear tabla silver_p filtrada desde capacity_current_customer_sku_status
from pyspark.sql.functions import col

# Nos aseguramos de que exista el schema silver_p
spark.sql("CREATE SCHEMA IF NOT EXISTS silver_p")

# Cargar tabla origen
sku_status_df = spark.table("capacity_current_customer_sku_status")

# Valores de interés en Current_Customer_SKU_Status
valores_objetivo = [
    "(3) Primarily P SKU w/ F SKU consumption",
    "(4) P and FT1 Only (Tested F SKU)"
]

sku_status_filtrado = sku_status_df.filter(col("Current_Customer_SKU_Status").isin(valores_objetivo))

# Guardar como nueva tabla en schema silver_p
sku_status_filtrado.write.mode("overwrite").saveAsTable("silver_p.capacity_current_customer_sku_status_filtered")

print("Tabla silver_p.capacity_current_customer_sku_status_filtered creada/actualizada correctamente.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sku = spark.sql("SELECT * FROM silver_p.capacity_current_customer_sku_status_filtered LIMIT 100")
display(df_sku)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Unir silver_p.capacity_accounts_usage_agg (principal) con silver_p.capacity_current_customer_sku_status_filtered
from pyspark.sql.functions import col

# Tablas base
agg_df = spark.table("silver_p.capacity_accounts_usage_agg")
sku_df = spark.table("silver_p.capacity_current_customer_sku_status_filtered")

# Join: tabla principal = agg_df
joined_df = (
    agg_df.alias("a")
    .join(
        sku_df.alias("s"),
        on=[
            col("a.Account_TPID") == col("s.Account_TPID"),
            col("a.TPID") == col("s.TPID")
        ],
        how="left"  # left join porque capacity_accounts_usage_agg es la principal
    )
)

# Seleccionar todas las columnas de la tabla principal (a.*)
# y todas las columnas de la tabla de estado (s.*) excepto las llaves duplicadas
cols_a = [col("a." + c) for c in agg_df.columns]
cols_s = [
    col("s." + c).alias("status_" + c)
    for c in sku_df.columns
    if c.lower() not in ["account_tpid", "tpid"]  # evitamos duplicar las llaves
]

joined_df_final = joined_df.select(*(cols_a + cols_s))

# Guardar como nueva tabla silver_p con todas las columnas
joined_df_final.write.mode("overwrite").saveAsTable("silver_p.capacity_accounts_usage_with_status")

print("Tabla silver_p.capacity_accounts_usage_with_status creada/actualizada correctamente.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_join = spark.sql("SELECT * FROM silver_p.capacity_accounts_usage_with_status LIMIT 100")
display(df_join)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cruce de silver_p.capacity_accounts_usage_with_status con msx_my_team_accounts
from pyspark.sql.functions import col, when

# Tablas base
usage_df = spark.table("silver_p.capacity_accounts_usage_with_status")
msx_df = spark.table("msx_my_team_accounts")

# Join por TPID (usage = principal)
joined_msx_df = (
    usage_df.alias("u")
    .join(
        msx_df.alias("m"),
        on=col("u.TPID") == col("m.TPID"),
        how="left"  # mantenemos todos los registros de usage_df
    )
)

# Seleccionar todas las columnas de usage + campos MSX requeridos
result_df = joined_msx_df.select(
    col("u.*"),
    col("m.Region").alias("Region"),
    col("m.Country").alias("Country"),
    col("m.Owner").alias("Owner"),
    col("m.Subsegment").alias("Subsegment"),
    col("m.Vertical").alias("Vertical"),
    col("m.SubVertical").alias("SubVertical"),
    col("m.Category").alias("Category"),
    col("m.Pinnacle_Account").alias("Pinnacle_Account"),
    # Flag para saber si es Pinnacle: cualquier valor no nulo se considera Pinnacle
    when(col("m.Pinnacle_Account").isNotNull(), True)
      .otherwise(False)
      .alias("IsPinnacle")
)

# Guardar como nueva tabla silver_p
result_df.write.mode("overwrite").saveAsTable("silver_p.capacity_accounts_usage_with_status_msx")

print("Tabla silver_p.capacity_accounts_usage_with_status_msx creada/actualizada correctamente.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final = spark.sql("SELECT * FROM silver_p.capacity_accounts_usage_with_status_msx LIMIT 100")
display(df_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
