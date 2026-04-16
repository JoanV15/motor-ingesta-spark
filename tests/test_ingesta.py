import json
from collections import namedtuple
from pathlib import Path
from motor_ingesta.motor_ingesta import MotorIngesta
from motor_ingesta.agregaciones import aniade_intervalos_por_aeropuerto, aniade_hora_utc
from pyspark.sql import functions as F


def test_aplana(spark):
    """
    Testea que el aplanado se haga correctamente con un DF creado ad-hoc
    :param spark: SparkSession configurada localmente
    """
    tupla3 = namedtuple("tupla3", ["a1", "a2", "a3"])
    tupla2 = namedtuple("tupla2", ["b1", "b2"])

    test_df = spark.createDataFrame(
        [(tupla3("a", "b", "c"), "hola", 3, [tupla2("pepe", "juan"), tupla2("pepito", "juanito")])],
        ["tupla", "nombre", "edad", "amigos"]
    )

    # Invocamos aal metodo Aplana de la clase Motor Ingesta
    aplanado_df = MotorIngesta.aplana_df(test_df)

    # Comprobamos que las columnas extraídas existen y las anidadas originales han desaparecido
    columnas = aplanado_df.columns
    for col_esperada in ["a1", "a2", "a3", "b1", "b2", "nombre", "edad"]:
        assert col_esperada in columnas

    assert "tupla" not in columnas
    assert "amigos" not in columnas


def test_ingesta_fichero(spark):
    """
    Comprueba que la ingesta de un fichero JSON de prueba se hace correctamente.
    :param spark: SparkSession inicializada localmente
    """
    carpeta_este_fichero = str(Path(__file__).parent)
    path_test_config = carpeta_este_fichero + "/resources/test_config.json"
    path_test_data = carpeta_este_fichero + "/resources/test_data.json"

    with open(path_test_config, "r") as f:
        config = json.load(f)

    # Creamos un objeto motor de ingesta
    motor_ingesta = MotorIngesta(config)

    # Ingestamos el fichero JSON
    datos_df = motor_ingesta.ingesta_fichero(path_test_data)

    # Checkeamos columnas
    columnas = datos_df.columns
    assert len(columnas) == 4
    for col_esperada in ["nombre", "parentesco", "numero", "profesion"]:
        assert col_esperada in columnas

    # Comprobamos valores de la primera fila según test_data.json
    primera_fila = datos_df.first()
    assert primera_fila.nombre == "Juan"
    assert primera_fila.parentesco == "sobrino"
    assert primera_fila.numero == 3
    assert primera_fila.profesion == "Ingeniero"


def test_aniade_intervalos_por_aeropuerto(spark):
    """
    Comprueba que las variables añadidas con información del vuelo inmediatamente posterior
    están bien calculadas
    :param spark: SparkSession inicializada localmente
    """
    test_df = spark.createDataFrame(
        [("JFK", "2023-12-25 15:35:00", "American_Airlines"),
         ("JFK", "2023-12-25 17:35:00", "Iberia")],
        ["Origin", "FlightTime", "Reporting_Airline"]
    ).withColumn("FlightTime", F.col("FlightTime").cast("timestamp"))

    # El siguiente vuelo sale 2 horas después (7200 segundos)
    expected_df = spark.createDataFrame(
        [("JFK", "2023-12-25 15:35:00", "American_Airlines", "2023-12-25 17:35:00", "Iberia", 7200)],
        ["Origin", "FlightTime", "Reporting_Airline", "FlightTime_next", "Airline_next", "diff_next"]
    ).withColumn("FlightTime", F.col("FlightTime").cast("timestamp")) \
        .withColumn("FlightTime_next", F.col("FlightTime_next").cast("timestamp"))

    expected_row = expected_df.first()

    result_df = aniade_intervalos_por_aeropuerto(test_df)

    # Ordenamos por FlightTime para coger el primer vuelo y comparar con su next
    actual_row = result_df.orderBy("FlightTime").first()

    # Comparar los campos clave
    assert actual_row.FlightTime_next == expected_row.FlightTime_next
    assert actual_row.Airline_next == expected_row.Airline_next
    assert actual_row.diff_next == expected_row.diff_next


def test_aniade_hora_utc(spark):
    """
    Comprueba que la columna FlightTime en la zona horaria UTC está correctamente calculada
    :param spark: SparkSession inicializada localmente
    """
    test_df = spark.createDataFrame(
        [("JFK", "2023-12-25", 1535)],
        ["Origin", "FlightDate", "DepTime"]
    )

    # JFK está en America/New_York (UTC-5 en diciembre).
    # Por tanto, las 15:35 locales son las 20:35 en UTC.
    expected_df = spark.createDataFrame(
        [("JFK", "2023-12-25", 1535, "2023-12-25 20:35:00")],
        ["Origin", "FlightDate", "DepTime", "FlightTime"]
    ).withColumn("FlightTime", F.col("FlightTime").cast("timestamp"))

    expected_row = expected_df.first()

    result_df = aniade_hora_utc(spark, test_df)
    actual_row = result_df.first()

    # Comparamos la marca de tiempo calculada
    assert actual_row.FlightTime == expected_row.FlightTime