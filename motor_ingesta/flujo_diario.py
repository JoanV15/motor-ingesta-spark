import json
from datetime import timedelta
from loguru import logger

from pyspark.sql import SparkSession, functions as F

from motor_ingesta.motor_ingesta import MotorIngesta
import motor_ingesta.agregaciones as agregaciones

class FlujoDiario:

    def __init__(self, config_file: str):
        """
        Inicializa el flujo diario leyendo la configuración y levantando la sesión adecuada.
        :param config_file: Ruta al fichero JSON de configuración.
        """
        with open(config_file, 'r') as f:
            self.config = json.load(f)

        # Creamos la sesión dinámicamente según el entorno
        if self.config.get("EXECUTION_ENVIRONMENT") == "databricks":
            from databricks.connect import DatabricksSession
            self.spark = DatabricksSession.builder.getOrCreate()
            logger.info("Iniciada DatabricksSession para desarrollo remoto.")
        else:
            self.spark = SparkSession.builder.getOrCreate()
            logger.info("Iniciada SparkSession local/producción.")

    def procesa_diario(self, data_file: str):
        """
        Ejecuta el flujo completo de ingesta, transformación y escritura de vuelos.
        :param data_file: Ruta del fichero JSON a procesar.
        """
        try:
            # Instanciamos el motor pasando la configuración
            motor = MotorIngesta(self.config)

            # Ingesta y cacheo inicial
            flights_df = motor.ingesta_fichero(data_file)
            flights_df.cache()

            # Añadimos la hora de salida UTC llamando a la función de agregaciones
            flights_with_utc = agregaciones.aniade_hora_utc(self.spark, flights_df)

            # -----------------------------
            #  CÓDIGO PARA EL EJERCICIO 4
            # -----------------------------
            dia_actual = flights_df.first().FlightDate
            dia_previo = dia_actual - timedelta(days=1)

            try:
                # Lectura de la tabla particionada
                flights_previo = self.spark.read.table(self.config["output_table"]).where(
                    F.col("FlightDate") == dia_previo)
                logger.info(f"Leída partición del día {dia_previo} con éxito")
            except Exception as e:
                logger.info(f"No se han podido leer datos del día {dia_previo} (Puede ser el primer día): {str(e)}")
                flights_previo = None

            if flights_previo:
                # Alineamos columnas de flights_with_utc para que coincidan con flights_previo
                for col_name, col_type in flights_previo.dtypes:
                    if col_name not in flights_with_utc.columns:
                        flights_with_utc = flights_with_utc.withColumn(col_name, F.lit(None).cast(col_type))

                # Reordenamos las columnas exactamente igual y unir
                flights_with_utc = flights_with_utc.select(flights_previo.columns)
                df_unido = flights_with_utc.unionByName(flights_previo)

                # Guardamos provisionalmente para romper el linaje y evitar error de lectura/escritura concurrente
                df_unido.write.mode("overwrite").saveAsTable("tabla_provisional")
                df_unido = self.spark.read.table("tabla_provisional")
            else:
                df_unido = flights_with_utc

            # Añadimos información del vuelo siguiente usando la función de agregaciones
            df_with_next_flight = agregaciones.aniade_intervalos_por_aeropuerto(df_unido)

            # Escribimos en la tabla
            df_with_next_flight \
                .coalesce(self.config["output_partitions"]) \
                .write \
                .mode("overwrite") \
                .option("partitionOverwriteMode", "dynamic") \
                .partitionBy("FlightDate") \
                .saveAsTable(self.config["output_table"])

            # Limpiamos
            self.spark.sql("DROP TABLE IF EXISTS tabla_provisional")
            logger.info(f"Procesamiento del fichero {data_file} completado")

        except Exception as e:
            logger.error(f"No se pudo escribir la tabla del fichero {data_file}")
            raise e


if __name__ == '__main__':
    # Bloque de prueba local
    flujo = FlujoDiario("config/config.json")
    flujo.procesa_diario("motor_ingesta/resources/vuelos_test.json")