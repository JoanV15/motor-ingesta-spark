from pathlib import Path
from pyspark.sql import SparkSession, DataFrame as DF, functions as F, Window
import pandas as pd


def aniade_hora_utc(spark: SparkSession, df: DF) -> DF:
    """
    Une los datos del vuelo con un catálogo de timezones para convertir
    la hora local de salida en una marca de tiempo UTC estandarizada.
    """
    path_timezones = str(Path(__file__).parent) + "/resources/timezones.csv"
    timezones_pd = pd.read_csv(path_timezones)
    timezones_df = spark.createDataFrame(timezones_pd)

    # Hacemos un LEFT JOIN para no perder vuelos si el aeropuerto no está en el CSV
    df_with_tz = df.join(timezones_df, df["Origin"] == timezones_df["iata_code"], "left")

    # Transformaciones del Ejercicio 2 encadenadas para que sea mas eficiente
    df_with_flight_time = df_with_tz.withColumn(
        "castedHour", F.lpad(F.col("DepTime").cast("string"), 4, "0")
    ).withColumn(
        "FlightTime",
        F.concat(
            F.col("FlightDate").cast("string"),
            F.lit(" "),
            F.col("castedHour").substr(1, 2),
            F.lit(":"),
            F.col("castedHour").substr(3, 2),
            F.lit(":00")  # Añadimos los segundos
        ).cast("timestamp")
    ).withColumn(
        "FlightTime",
        F.to_utc_timestamp(F.col("FlightTime"), F.col("iana_tz"))
    )

    # (d) Borramos las columnas auxiliares del join y del cálculo temporal
    cols_to_drop = timezones_df.columns + ["castedHour"]
    df_with_flight_time = df_with_flight_time.drop(*cols_to_drop)

    return df_with_flight_time


def aniade_intervalos_por_aeropuerto(df: DF) -> DF:
    """
    Añade información del vuelo posterior (mismo aeropuerto)
    y calcula el tiempo de espera entre ambos.
    """
    #  Hacemos particiones por el mismo aeropuerto de origen y ordenamos cronológicamente
    w = Window.partitionBy("Origin").orderBy("FlightTime")

    df_with_next_flight = df.withColumn(
        "temp_tuple", F.struct("FlightTime", "Reporting_Airline")
    ).withColumn(
        # Usamos lag(..., -1) para traer el registro "siguiente"
        "next_flight_tuple", F.lag(F.col("temp_tuple"), -1).over(w)
    ).withColumn(
        "FlightTime_next", F.col("next_flight_tuple.FlightTime")
    ).withColumn(
        "Airline_next", F.col("next_flight_tuple.Reporting_Airline")
    ).withColumn(
        "diff_next",
        # Hacemos la diferencia matemática convirtiendo el timestamp a Long (Unix Epoch Seconds)
        F.col("FlightTime_next").cast("long") - F.col("FlightTime").cast("long")
    ).drop("temp_tuple", "next_flight_tuple")

    return df_with_next_flight