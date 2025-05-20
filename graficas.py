from ElasticSearchProvider import ElasticSearchProvider
import json
import pandas as pd
import re
import matplotlib.pyplot as plt

name_index = "datos-accidentes-chicago"

#Grafica de accidentes en base al cambio climatico
try:
    with ElasticSearchProvider(index=name_index) as es:
        response = es.connection.search(index=name_index, body= {
            "size": 0,
            "aggs": {
                "WEATHER_CONDITION": {
                    "terms": {
                        "field": "WEATHER_CONDITION",
                        "size": 32
                    }
                }
            }
        })
        print(json.dumps(response.body, indent=2))

        buckets = response["aggregations"]["WEATHER_CONDITION"]["buckets"]
        labels = [bucket["key"] for bucket in buckets]
        counts = [bucket["doc_count"] for bucket in buckets]

        plt.figure(figsize=(14, 6))
        bars = plt.barh(labels, counts, color='skyblue')
        plt.xlabel("Cantidad de accidentes")
        plt.ylabel("Condición del clima")
        plt.title("Accidentes por condición climática")
        plt.tight_layout()
        for bar in bars:
            width = bar.get_width()
            plt.text(width + 50, bar.get_y() + bar.get_height() / 2,
                     f'{int(width)}', va='center', fontsize=8)
        plt.show()
except Exception as e:
    print(f"Ocurrio un error: {e}")

#Grafica de cantidad de accidentes por año
try:
    with ElasticSearchProvider(index=name_index) as es:
        response = es.connection.search(index=name_index, body={
            "size": 0,
            "aggs": {
                "accidentes_por_año": {
                    "date_histogram": {
                        "field": "CRASH_DATE",
                        "calendar_interval": "year",
                        "format": "yyyy"
                    }
                }
            }
        })

        # Extraer datos
        buckets = response.body["aggregations"]["accidentes_por_año"]["buckets"]
        años = [bucket["key_as_string"] for bucket in buckets]
        conteos = [bucket["doc_count"] for bucket in buckets]

        # Graficar
        plt.figure(figsize=(10, 6))
        bars = plt.bar(años, conteos, color="orange")
        plt.xlabel("Año")
        plt.ylabel("Cantidad de accidentes")
        plt.title("Accidentes por año")
        plt.tight_layout()

        # Agregar etiquetas de cantidad encima de cada barra
        for bar, count in zip(bars, conteos):
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height(), str(count),
                     ha='center', va='bottom', fontsize=9)

        plt.show()

except Exception as e:
    print(f"Ocurrió un error: {e}")

#Grafica de tipos de accidentes en el año 2018
try:
    with ElasticSearchProvider(index=name_index) as es:
        response = es.connection.search(index=name_index, body={
            "size": 0,
            "query": {
                "range": {
                    "CRASH_DATE": {
                        "gte": "2018-01-01",
                        "lte": "2018-12-31",
                        "format": "yyyy-MM-dd"
                    }
                }
            },
            "aggs": {
                "FIRST_CRASH_TYPE": {
                    "terms": {
                        "field": "FIRST_CRASH_TYPE",
                        "size": 30
                    }
                }
            }
        })

        # Extraer datos
        buckets = response.body["aggregations"]["FIRST_CRASH_TYPE"]["buckets"]
        tipos = [bucket["key"] for bucket in buckets]
        conteos = [bucket["doc_count"] for bucket in buckets]

        # Graficar
        plt.figure(figsize=(12, 6))
        bars = plt.barh(tipos, conteos, color="steelblue")
        plt.xlabel("Cantidad de accidentes")
        plt.ylabel("Tipo de accidente")
        plt.title("Tipos de accidentes en 2018")
        plt.tight_layout()

        # Mostrar cantidad en la barra
        for bar, count in zip(bars, conteos):
            plt.text(bar.get_width(), bar.get_y() + bar.get_height() / 2,
                     str(count), va="center", ha="left", fontsize=8)

        plt.show()

except Exception as e:
    print(f"Ocurrió un error: {e}")

#Cantidad de accidentes por número de unidades de golpe trasero en el año 2018
try:
    with ElasticSearchProvider(index=name_index) as es:
        response = es.connection.search(index=name_index, body={
            "size": 0,
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "CRASH_DATE": {
                                    "gte": "2018-01-01",
                                    "lte": "2018-12-31",
                                    "format": "yyyy-MM-dd"
                                }
                            }
                        },
                        {
                            "term": {
                                "FIRST_CRASH_TYPE": "REAR END"
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "por_numero_de_unidades": {
                    "terms": {
                        "field": "NUM_UNITS",
                        "size": 20,
                        "order": { "_key": "asc" }
                    }
                }
            }
        })

        # Obtener resultados
        buckets = response.body["aggregations"]["por_numero_de_unidades"]["buckets"]

        # Imprimir desglose en consola
        print("Desglose de accidentes 'REAR END' en 2018 por número de unidades:")
        for bucket in buckets:
            print(f"{bucket['key']} unidades: {bucket['doc_count']} accidentes")

        # Graficar
        labels = [str(bucket["key"]) for bucket in buckets]
        counts = [bucket["doc_count"] for bucket in buckets]

        plt.figure(figsize=(10, 6))
        bars = plt.bar(labels, counts, color='teal')
        plt.xlabel("Número de unidades involucradas")
        plt.ylabel("Cantidad de accidentes")
        plt.title("Accidentes 'REAR END' por número de unidades (2018)")

        # Mostrar valores sobre las barras
        for bar, count in zip(bars, counts):
            plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), str(count),
                     ha='center', va='bottom', fontsize=9)

        plt.tight_layout()
        plt.show()

except Exception as e:
    print(f"Ocurrió un error: {e}")

#Grafica de la cantidad de Lesionados de gravedad por número de unidades de golpe trasero en el año 2018
try:
    with ElasticSearchProvider(index=name_index) as es:
        response = es.connection.search(index=name_index, body={
            "size": 0,
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "CRASH_DATE": {
                                    "gte": "2018-01-01",
                                    "lte": "2018-12-31",
                                    "format": "yyyy-MM-dd"
                                }
                            }
                        },
                        {
                            "term": {
                                "FIRST_CRASH_TYPE": "REAR END"
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "NUM_UNITS": {
                    "terms": {
                        "field": "NUM_UNITS",
                        "size": 20,
                        "order": { "INJURIES_INCAPACITATING": "desc" }
                    },
                    "aggs": {
                        "INJURIES_INCAPACITATING": {
                            "sum": {
                                "field": "INJURIES_INCAPACITATING"
                            }
                        }
                    }
                }
            }
        })

        # Extraer los resultados
        buckets = response.body["aggregations"]["NUM_UNITS"]["buckets"]
        unidades = [str(bucket["key"]) for bucket in buckets]
        fallecidos = [int(bucket["INJURIES_INCAPACITATING"]["value"]) for bucket in buckets]

        # Mostrar por consola
        print("Lesionados de gravedad por número de unidades (REAR END - 2018):")
        for u, f in zip(unidades, fallecidos):
            print(f"Unidades: {u}, lesionados de gravedad: {f}")

        # Graficar
        plt.figure(figsize=(10, 6))
        bars = plt.bar(unidades, fallecidos, color="purple")
        plt.xlabel("Número de unidades involucradas")
        plt.ylabel("Cantidad de lesionados de gravedad")
        plt.title("Lesionados de gravedad por accidentes REAR END en 2018 según unidades involucradas")
        plt.xticks(rotation=45)

        # Etiquetas encima de cada barra
        for bar in bars:
            yval = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2, yval + 0.1, yval, ha='center', va='bottom')

        plt.tight_layout()
        plt.show()

except Exception as e:
    print(f"Ocurrió un error: {e}")

#Grafica de cantidad de fallecidos dependiendo de las unidades involucradas de tipo de golpe trasero en el año 2018
try:
    with ElasticSearchProvider(index=name_index) as es:
        response = es.connection.search(index=name_index, body={
            "size": 0,
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "CRASH_DATE": {
                                    "gte": "2018-01-01",
                                    "lte": "2018-12-31",
                                    "format": "yyyy-MM-dd"
                                }
                            }
                        },
                        {
                            "term": {
                                "FIRST_CRASH_TYPE": "REAR END"
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "NUM_UNITS": {
                    "terms": {
                        "field": "NUM_UNITS",
                        "size": 20,
                        "order": { "INJURIES_FATAL": "desc" }
                    },
                    "aggs": {
                        "INJURIES_FATAL": {
                            "sum": {
                                "field": "INJURIES_FATAL"
                            }
                        }
                    }
                }
            }
        })

        # Extraer los resultados
        buckets = response.body["aggregations"]["NUM_UNITS"]["buckets"]
        unidades = [str(bucket["key"]) for bucket in buckets]
        fallecidos = [int(bucket["INJURIES_FATAL"]["value"]) for bucket in buckets]

        # Mostrar por consola
        print("Fallecidos por número de unidades (REAR END - 2018):")
        for u, f in zip(unidades, fallecidos):
            print(f"Unidades: {u}, Fallecidos: {f}")

        # Graficar
        plt.figure(figsize=(10, 6))
        bars = plt.bar(unidades, fallecidos, color="salmon")
        plt.xlabel("Número de unidades involucradas")
        plt.ylabel("Cantidad de fallecidos")
        plt.title("Fallecidos por accidentes REAR END en 2018 según unidades involucradas")
        plt.xticks(rotation=45)

        # Etiquetas encima de cada barra
        for bar in bars:
            yval = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2, yval + 0.1, yval, ha='center', va='bottom')

        plt.tight_layout()
        plt.show()

except Exception as e:
    print(f"Ocurrió un error: {e}")


#Grafica del número de accidentes por tipo de causa

try:
    with ElasticSearchProvider(index=name_index) as es:
        response = es.connection.search(index=name_index, body={
            "size": 0,
            "aggs": {
                "PRIM_CONTRIBUTORY_CAUSE": {
                    "terms": {
                        "field": "PRIM_CONTRIBUTORY_CAUSE.keyword",
                        "size": 10,  # puedes aumentar el size si hay muchas causas
                        "order": { "_count": "desc" }
                    }
                }
            }
        })

        # Extraer datos
        buckets = response.body["aggregations"]["PRIM_CONTRIBUTORY_CAUSE"]["buckets"]
        causas = [bucket["key"] for bucket in buckets]
        cantidades = [bucket["doc_count"] for bucket in buckets]

        # Mostrar por consola
        print("Número de accidentes por causa:")
        for c, q in zip(causas, cantidades):
            print(f"{c}: {q}")

        # Graficar
        plt.figure(figsize=(12, 8))
        bars = plt.barh(causas, cantidades, color='cornflowerblue')
        plt.xlabel("Cantidad de accidentes")
        plt.ylabel("Causa primaria")
        plt.title("Accidentes por causa primaria")
        plt.gca().invert_yaxis()  # para mostrar la barra más grande arriba

        # Etiquetas sobre cada barra
        for bar in bars:
            width = bar.get_width()
            plt.text(width + 10, bar.get_y() + bar.get_height() / 2,
                     str(width), va='center')

        plt.tight_layout()
        plt.show()

except Exception as e:
    print(f"Ocurrió un error: {e}")

#Grafica del número de accidentes por condición de la via
try:
    with ElasticSearchProvider(index=name_index) as es:
        response = es.connection.search(index=name_index, body={
            "size": 0,
            "aggs": {
                "condiciones_via": {
                    "terms": {
                        "field": "ROADWAY_SURFACE_COND",
                        "size": 10
                    },
                    "aggs": {
                        "por_año": {
                            "date_histogram": {
                                "field": "CRASH_DATE",
                                "calendar_interval": "year",
                                "format": "yyyy"
                            }
                        }
                    }
                }
            }
        })
        #Extraer datos
        buckets = response.body["aggregations"]["condiciones_via"]["buckets"]
        tipos = [bucket["key"] for bucket in buckets]
        conteos = [bucket["doc_count"] for bucket in buckets]

        # Graficar
        plt.figure(figsize=(12, 6))
        bars = plt.barh(tipos, conteos, color="steelblue")
        plt.xlabel("Cantidad de accidentes")
        plt.ylabel("Condición de via")
        plt.title("Accidentes por condición de via agrupados por año")
        plt.tight_layout()

        # Mostrar cantidad en la barra
        for bar, count in zip(bars, conteos):
            plt.text(bar.get_width(), bar.get_y() + bar.get_height() / 2,
                     str(count), va="center", ha="left", fontsize=8)

        plt.show()

except Exception as e:
    print(f"Ocurrió un error: {e}")

#Grafica del número de accidentes por defecto de la via
try:
    with ElasticSearchProvider(index=name_index) as es:
        response = es.connection.search(index=name_index, body={
            "size": 0,
            "aggs": {
                "defecto_via": {
                    "terms": {
                        "field": "ROAD_DEFECT",
                        "size": 10
                    }
                }
            }
        })
        #Extraer datos
        buckets = response.body["aggregations"]["defecto_via"]["buckets"]
        tipos = [bucket["key"] for bucket in buckets]
        conteos = [bucket["doc_count"] for bucket in buckets]

        # Graficar
        plt.figure(figsize=(12, 6))
        bars = plt.barh(tipos, conteos, color="gray")
        plt.xlabel("Cantidad de accidentes")
        plt.ylabel("Defecto de la via")
        plt.title("Accidentes por defecto de la via")
        plt.tight_layout()

        # Mostrar cantidad en la barra
        for bar, count in zip(bars, conteos):
            plt.text(bar.get_width(), bar.get_y() + bar.get_height() / 2,
                     str(count), va="center", ha="left", fontsize=8)

        plt.show()

except Exception as e:
    print(f"Ocurrió un error: {e}")

#Grafica del número de accidentes por iluminación de la via
try:
    with ElasticSearchProvider(index=name_index) as es:
        response = es.connection.search(index=name_index, body={
            "size": 0,
            "aggs": {
                "LIGHTING_CONDITION": {
                    "terms": {
                        "field": "LIGHTING_CONDITION",
                        "size": 10
                    }
                }
            }
        })
        #Extraer datos
        buckets = response.body["aggregations"]["LIGHTING_CONDITION"]["buckets"]
        tipos = [bucket["key"] for bucket in buckets]
        conteos = [bucket["doc_count"] for bucket in buckets]

        # Graficar
        plt.figure(figsize=(12, 6))
        bars = plt.barh(tipos, conteos, color="yellowgreen")
        plt.xlabel("Cantidad de accidentes")
        plt.ylabel("Iluminación de la via")
        plt.title("Accidentes por iluminación de la via")
        plt.tight_layout()

        # Mostrar cantidad en la barra
        for bar, count in zip(bars, conteos):
            plt.text(bar.get_width(), bar.get_y() + bar.get_height() / 2,
                     str(count), va="center", ha="left", fontsize=8)

        plt.show()

except Exception as e:
    print(f"Ocurrió un error: {e}")


#Grafica del número de accidentes entre las 7 am y 9 am.
try:
    with ElasticSearchProvider(index=name_index) as es:
        response = es.connection.search(index=name_index, body={
            "size": 0,
            "query": {
                "range": {
                    "CRASH_HOUR": {
                        "gte": 7,
                        "lte": 9
                    }
                }
            },
            "aggs": {
                "horas": {
                    "terms": {
                        "field": "CRASH_HOUR",
                        "size": 3,
                        "order": { "_key": "asc" }
                    }
                }
            }
        })

        # Extraer datos
        buckets = response.body["aggregations"]["horas"]["buckets"]
        tipos = [bucket["key"] for bucket in buckets]
        conteos = [bucket["doc_count"] for bucket in buckets]

        # Graficar
        plt.figure(figsize=(8, 5))
        bars = plt.barh([f"{h}:00" for h in tipos], conteos, color="navy")
        plt.xlabel("Cantidad de accidentes")
        plt.ylabel("Hora del día")
        plt.title("Accidentes entre 7 y 9 AM")
        plt.tight_layout()

        # Mostrar cantidad en la barra
        for bar, count in zip(bars, conteos):
            plt.text(bar.get_width(), bar.get_y() + bar.get_height() / 2,
                     str(count), va="center", ha="left", fontsize=9)

        plt.show()

except Exception as e:
    print(f"Ocurrió un error: {e}")


# Grafica de las 5 calles con mas accidentes. 
try:
    with ElasticSearchProvider(index=name_index) as es:
        response = es.connection.search(index=name_index, body={
            "size": 0,
            "aggs": {
                "STREET_NAME": {
                    "terms": {
                        "field": "STREET_NAME.keyword",  # Asegúrate de usar el campo .keyword si STREET_NAME es texto
                        "size": 5,
                        
                    }
                }
            }
        })

        # Extraer datos
        buckets = response.body["aggregations"]["STREET_NAME"]["buckets"]
        calles = [bucket["key"] for bucket in buckets]
        conteos = [bucket["doc_count"] for bucket in buckets]

        # Graficar
        plt.figure(figsize=(10, 5))
        bars = plt.barh(calles, conteos, color="darkred")
        plt.xlabel("Cantidad de accidentes")
        plt.ylabel("Nombre de calle")
        plt.title("Top 5 calles con más accidentes")
        plt.tight_layout()

        # Mostrar cantidad en la barra
        for bar, count in zip(bars, conteos):
            plt.text(bar.get_width(), bar.get_y() + bar.get_height() / 2,
                     str(count), va="center", ha="left", fontsize=9)

        plt.show()

except Exception as e:
    print(f"Ocurrió un error: {e}")

#Grafica de la gravedad de los accidentes
try:
    with ElasticSearchProvider(index=name_index) as es:
        response = es.connection.search(index=name_index, body={
            "size": 0,
            "aggs": {
                "CRASH_TYPE": {
                    "terms": {
                        "field": "CRASH_TYPE",  # Asegúrate de usar el campo .keyword si STREET_NAME es texto
                        "size": 10,
                        
                    }
                }
            }
        })

        # Extraer datos
        buckets = response.body["aggregations"]["CRASH_TYPE"]["buckets"]
        calles = [bucket["key"] for bucket in buckets]
        conteos = [bucket["doc_count"] for bucket in buckets]

        # Graficar
        plt.figure(figsize=(10, 5))
        bars = plt.barh(calles, conteos, color="indigo")
        plt.xlabel("Cantidad de accidentes")
        plt.ylabel("Nivel de gravedad")
        plt.title("Gravedad de los accidentes")
        plt.tight_layout()

        # Mostrar cantidad en la barra
        for bar, count in zip(bars, conteos):
            plt.text(bar.get_width(), bar.get_y() + bar.get_height() / 2,
                     str(count), va="center", ha="left", fontsize=9)

        plt.show()

except Exception as e:
    print(f"Ocurrió un error: {e}")



#Grafica de la forma de la via donde surgio el accidente
try:
    with ElasticSearchProvider(index=name_index) as es:
        response = es.connection.search(index=name_index, body={
            "size": 0,
            "aggs": {
                "ALIGNMENT": {
                    "terms": {
                        "field": "ALIGNMENT",  # Asegúrate de usar el campo .keyword si STREET_NAME es texto
                        "size": 10,
                        
                    }
                }
            }
        })

        # Extraer datos
        buckets = response.body["aggregations"]["ALIGNMENT"]["buckets"]
        calles = [bucket["key"] for bucket in buckets]
        conteos = [bucket["doc_count"] for bucket in buckets]

        # Graficar
        plt.figure(figsize=(10, 5))
        bars = plt.barh(calles, conteos, color="skyblue")
        plt.xlabel("Cantidad de accidentes")
        plt.ylabel("Forma de la via")
        plt.title("Cantidad de accidentes en base al tipo de via")
        plt.tight_layout()

        # Mostrar cantidad en la barra
        for bar, count in zip(bars, conteos):
            plt.text(bar.get_width(), bar.get_y() + bar.get_height() / 2,
                     str(count), va="center", ha="left", fontsize=9)

        plt.show()

except Exception as e:
    print(f"Ocurrió un error: {e}")


#Grafica sobre la cantidad de accidentes del viernes al domingo
try:
    with ElasticSearchProvider(index=name_index) as es:
        response = es.connection.search(index=name_index, body={
            "size": 0,
            "query": {
                "terms": {
                    "CRASH_DAY_OF_WEEK": [5, 6, 7]
                }
            },
            "aggs": {
                "CRASH_DAY_OF_WEEK": {
                    "terms": {
                        "field": "CRASH_DAY_OF_WEEK",
                        "size": 3,
                        "order": {
                            "_key": "asc"
                        }
                    }
                }
            }
        })

        buckets = response.body["aggregations"]["CRASH_DAY_OF_WEEK"]["buckets"]
        dias = [bucket["key"] for bucket in buckets]
        conteos = [bucket["doc_count"] for bucket in buckets]

        # Opcional: Mapeo de número a nombre de día
        nombres_dia = {5: "Viernes", 6: "Sábado", 7: "Domingo"}
        etiquetas = [nombres_dia.get(d, str(d)) for d in dias]

        # Graficar
        plt.figure(figsize=(8, 5))
        bars = plt.bar(etiquetas, conteos, color="darkorange")
        plt.xlabel("Día de la semana")
        plt.ylabel("Cantidad de accidentes")
        plt.title("Accidentes de tráfico (viernes a domingo)")
        plt.tight_layout()

        for bar, count in zip(bars, conteos):
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                     str(count), ha='center', va='bottom', fontsize=9)

        plt.show()

except Exception as e:
    print(f"Ocurrió un error: {e}")
