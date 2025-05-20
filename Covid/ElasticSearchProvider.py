from elasticsearch import Elasticsearch, helpers
import json
import uuid

class ElasticSearchProvider:

    def __init__(self,index):
        self.host = 'http://localhost:9200/'
        # self.user = stri{user}
        # self.password = str{password}
        self.index = index
        self.index_type = '_doc'
        self.connection = Elasticsearch(self.host,timeout=120,retry_on_timeout=True,max_retries=10)
        

    
    def __enter__(self):
        try:
            self.connection = Elasticsearch(self.host)
            return self
        except Exception as e:
            return{
                "StatusCode": 500,
                "body": json.dumps({
                    "message": str(e),
                }),
            }
    def __exit__(self, exeption_type, exception_value, traceback):
        self.connection.transport.close()

        
    # Función para verificar la conexión de ElasticSearch
    def check_connection(self):
        try:
            if self.connection.ping():
                return True
            else:
                return False
        except Exception as e:
            return{"StatusCode": 500,
            "body": json.dumps({
                "message": str(e),
            })}
    #Muestra todos los indices existentes de ElasticSearch
    def show_indices(self):
        try:
            response = self.connection.indices.get_alias(index="*").body
            return response
        except Exception as e:
            return{
                "StatusCode": 500,
                "body": json.dumps({
                    "message": str(e),
                }),
            }

    #Función para crear un nuevo indice
    def create_index(self, mapping):
        try:
            if  not self.connection.indices.exists(index=self.index):
                response = self.connection.indices.create(index=self.index, body=mapping)
            else:
                response = {
                    "StatusCode": 409,
                    "body": json.dumps({
                        "message": f"Index {self.index} ya existe"
                    })
                }
            return response
        except Exception as e:
            return {
                "StatusCode": 500,
                "body": json.dumps({
                    "message": str(e)
                    })
            }

    #Función para verificar el mapping del indice creado
    def get_mapping(self):
        try:
            response = self.connection.indices.get_mapping(index=self.index)
            return response
        except Exception as e:
            return {
                "StatusCode": 500,
                "body": json.dumps({
                    "message": str(e)
                    })
            }
    
    #Función para insertar los documentos a elasticsearch
    def insert_doc(self,datos):
        for doc in datos:
            yield {
                "_index": self.index,
                "_source": doc
            }

    #Función para guardar los documentos en grupos de 5000 para luego insertarlos a elasticsearch
    def insert_batch(self, datos):
        try:
            batchsize = 5000
            total = len(datos)
            for i in range(0, total, batchsize):
                batch = datos[i:i+batchsize]
                acciones = list(self.insert_doc(batch))
                success, failed = helpers.bulk(self.connection, acciones, raise_on_error=False, stats_only=False)
                print(f"Grupo{i// batchsize+1}, {success} documentos insertados correctamente.")

                if failed:
                    print(f"{len(failed)} documentos fallaron al insertarse.")
                    for error in failed:
                        print(f"Error en documento: {error}")
            
            return {
                "StatusCode": 200,
                "body": json.dumps({
                    "message": f"{total} documentos insertados en Elasticsearch."
                })
            }
        except Exception as e:
            return {
                "StatusCode": 500,
                "body": json.dumps({
                    "message": str(e)
                })
            }
    def documents(self):
        try:
            response = self.connection.search(index=self.index, body={"query": {"match_all": {}}})
            return response.body
        except Exception as e:
            return {
                "StatusCode": 500,
                "body": json.dumps({
                    "message": str(e),
                })
            }

   