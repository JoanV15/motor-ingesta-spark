import json
from pyspark.sql import DataFrame as DF, functions as F, SparkSession


class MotorIngesta:
    """
    Clase encargada de leer ficheros JSON, aplanar su estructura recursivamente
    y aplicar un esquema dinámico seleccionando las columnas configuradas.
    """

    def __init__(self, config: dict):
        """
        Inicializa el motor de ingesta con la configuración proporcionada.
        :param config: Diccionario con la configuración del proyecto (incluye 'data_columns').
        """
        self.config = config
        # getOrCreate() recupera automáticamente la DatabricksSession o SparkSession que creamos en FlujoDiario
        self.spark = SparkSession.builder.getOrCreate()

    def ingesta_fichero(self, json_path: str) -> DF:
        """
        Lee el fichero JSON, lo aplana y selecciona las columnas requeridas casteándolas.
        :param json_path: Ruta del JSON a procesar.
        :return: DataFrame limpio y tipado.
        """
        # Leemos el JSON
        flights_day_df = self.spark.read.json(json_path)

        # Aplanamos las estructuras anidadas
        aplanado_df = MotorIngesta.aplana_df(flights_day_df)

        # Creamos la lista de objetos Column usando comprensión de listas
        lista_obj_column = [
            F.col(diccionario["name"])
            .cast(diccionario["type"])
            .alias(diccionario["name"], metadata={"comment": diccionario.get("comment", "")})
            for diccionario in self.config["data_columns"]
        ]

        resultado_df = aplanado_df.select(*lista_obj_column)
        return resultado_df

    @staticmethod
    def aplana_df(df: DF) -> DF:
        """
        Aplana un DataFrame de Spark que tenga columnas de tipo array y de tipo estructura.
        """
        to_select = []
        schema = df.schema.jsonValue()
        fields = schema["fields"]
        recurse = False

        for f in fields:
            if f["type"].__class__.__name__ != "dict":
                to_select.append(f["name"])
            else:
                if f["type"]["type"] == "array":
                    to_select.append(F.explode(f["name"]).alias(f["name"]))
                    recurse = True
                elif f["type"]["type"] == "struct":
                    to_select.append(f"{f['name']}.*")
                    recurse = True

        new_df = df.select(*to_select)
        return MotorIngesta.aplana_df(new_df) if recurse else new_df