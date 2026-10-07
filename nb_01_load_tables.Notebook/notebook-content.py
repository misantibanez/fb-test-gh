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

# Cargar hojas específicas de los archivos Excel en tablas del Lakehouse

import pandas as pd

base_path = "/lakehouse/default/Files"

# Rutas de los archivos en OneLake (carpeta Files del lakehouse por defecto)
capacity_overview_path = f"{base_path}/capacity metrics/Capacity Overview (2).xlsx"
my_team_accounts_path = f"{base_path}/msx/My Team Accounts 9-22-2026 9-38-17 AM.xlsx"


def sanitize_columns(pdf: pd.DataFrame) -> pd.DataFrame:
    """Normaliza nombres de columnas para que Delta los acepte.
    - Quita espacios al inicio/fin
    - Sustituye espacios y caracteres conflictivos por '_'
    - Opcional: pasa todo a minúsculas
    """
    def _clean(col: str) -> str:
        if col is None:
            return col
        col = str(col).strip()
        # Reemplazar caracteres problemáticos: espacio, punto, paréntesis, igual, tab, salto de línea, etc.
        for ch in [" ", ".", ",", ";", "{", "}", "(", ")", "\n", "\t", "="]:
            col = col.replace(ch, "_")
        # Evitar múltiples guiones bajos seguidos
        while "__" in col:
            col = col.replace("__", "_")
        return col

    pdf = pdf.copy()
    pdf.columns = [_clean(c) for c in pdf.columns]
    return pdf


# 1) CAPACITY OVERVIEW -> 2 tablas (hojas: All Accounts Usage, Current Customer SKU Status)
capacity_sheets = {
    "capacity_all_accounts_usage": "All Accounts Usage",
    "capacity_current_customer_sku_status": "Current Customer SKU Status",
}

for table_name, sheet_name in capacity_sheets.items():
    pdf = pd.read_excel(capacity_overview_path, sheet_name=sheet_name)
    pdf = sanitize_columns(pdf)
    df = spark.createDataFrame(pdf)
    # Guardar como tabla en el lakehouse por defecto
    df.write.mode("overwrite").saveAsTable(table_name)


# 2) MY TEAM ACCOUNTS ->
#    - msx_my_team_accounts: hoja "My Team Accounts" completa
#    - msx_pinnacles: primera tabla de la hoja "Pinnacles" (columnas A:E)
#    - msx_foco: segunda tabla de la hoja "Pinnacles" (columnas a partir de la 7, por ejemplo G:J)

# 2.1) Hoja My Team Accounts -> una tabla
pdf_my_team = pd.read_excel(my_team_accounts_path, sheet_name="My Team Accounts")
pdf_my_team = sanitize_columns(pdf_my_team)
df_my_team = spark.createDataFrame(pdf_my_team)
df_my_team.write.mode("overwrite").saveAsTable("msx_my_team_accounts")

# 2.2) Hoja Pinnacles -> dos tablas lógicas
# Primera tabla: columnas A:E
pdf_pinnacles_1 = pd.read_excel(
    my_team_accounts_path,
    sheet_name="Pinnacles",
    usecols="A:E"
)
pdf_pinnacles_1 = sanitize_columns(pdf_pinnacles_1)
df_pinnacles_1 = spark.createDataFrame(pdf_pinnacles_1)
df_pinnacles_1.write.mode("overwrite").saveAsTable("msx_pinnacles")

# Segunda tabla (FOCO): columnas de la segunda tabla, a partir de la columna 7.
# Ajusta el rango de columnas si en el Excel son otras letras.
pdf_pinnacles_foco = pd.read_excel(
    my_team_accounts_path,
    sheet_name="Pinnacles",
    usecols="G:J"  # suponiendo que la 2ª tabla va de la columna 7 (G) a la 10 (J)
)
pdf_pinnacles_foco = sanitize_columns(pdf_pinnacles_foco)
df_pinnacles_foco = spark.createDataFrame(pdf_pinnacles_foco)
df_pinnacles_foco.write.mode("overwrite").saveAsTable("msx_foco")

print("Tablas creadas/actualizadas en el lakehouse:")
for t in [
    "capacity_all_accounts_usage",
    "capacity_current_customer_sku_status",
    "msx_my_team_accounts",
    "msx_pinnacles",
    "msx_foco",
]:
    print(" -", t)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Regla general: en todas las tablas no puede haber NULL en TPID / TPID_1 (ni variantes Account_TPID)

from pyspark.sql.functions import col

# Lista de tablas que queremos limpiar
tables = [
    "capacity_all_accounts_usage",
    "capacity_current_customer_sku_status",
    "msx_my_team_accounts",
    "msx_pinnacles",
    "msx_foco",
]

# Posibles nombres de columna de TPID
possible_tpid_cols = [
    "TPID",
    "TPID_1",
    "Account_TPID",
    "Account_TPID_1",
]

for table_name in tables:
    df = spark.table(table_name)

    # Detectar qué columnas TPID existen en la tabla
    present_tpid_cols = [c for c in possible_tpid_cols if c in df.columns]

    if not present_tpid_cols:
        print(f"[AVISO] En la tabla {table_name} no se encontró ninguna columna TPID/TPID_1/Account_TPID. No se aplica filtro.")
        continue

    before_count = df.count()

    # Aplicar filtro de no nulo para todas las columnas TPID presentes
    df_clean = df
    for c in present_tpid_cols:
        df_clean = df_clean.filter(col(c).isNotNull())

    after_count = df_clean.count()

    # Sobrescribir la tabla en el lakehouse
    df_clean.write.mode("overwrite").saveAsTable(table_name)

    print(f"Tabla: {table_name}")
    print(f" - Columnas TPID usadas para filtro: {present_tpid_cols}")
    print(f" - Filas antes: {before_count}")
    print(f" - Filas después (sin NULL en TPID/TPID_1): {after_count}")
    print("---")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

display(spark.table("capacity_all_accounts_usage"))
display(spark.table("capacity_current_customer_sku_status"))
display(spark.table("msx_my_team_accounts"))
display(spark.table("msx_pinnacles"))
display(spark.table("msx_foco"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
