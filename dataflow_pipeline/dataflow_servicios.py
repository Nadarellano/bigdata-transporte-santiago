import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import argparse
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)

# Definir el esquema de la tabla de BigQuery
table_schema = {
    'fields': [
        {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'tipoDia', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'inicio', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'fin', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'paradero_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'paradero_cod', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'paradero_num', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'paradero_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'paradero_comuna', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'paradero_latitud', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'paradero_longitud', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'servicio_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'servicio_cod', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'servicio_destino', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'servicio_orden', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'servicio_color', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'empresa_nombre', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'empresa_color', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'recorrido_destino', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'itinerario', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
        {'name': 'codigo_servicio', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    ]
}

class ExtractAndTransformData(beam.DoFn):
    def process(self, element):
        import json
        from datetime import datetime
        try:
            logging.info(f"Processing element: {element}")
            row = json.loads(element)
            id_value = row.get('ida', {}).get('id', None)
            horarios = row.get('ida', {}).get('horarios', [])
            paraderos = row.get('ida', {}).get('paraderos', [])
            timestamp = datetime.utcnow().isoformat()
            
            for horario in horarios:
                tipoDia = horario.get('tipoDia', None)
                inicio = horario.get('inicio', None)
                fin = horario.get('fin', None)
                
                for paradero in paraderos:
                    paradero_id = paradero.get('id', None)
                    paradero_cod = paradero.get('cod', None)
                    paradero_num = paradero.get('num', None)
                    paradero_name = paradero.get('name', None)
                    paradero_comuna = paradero.get('comuna', None)
                    paradero_latitud = paradero.get('pos', [None, None])[0]
                    paradero_longitud = paradero.get('pos', [None, None])[1]

                    servicios = paradero.get('servicios', [])
                    for servicio in servicios:
                        servicio_id = servicio.get('id', None)
                        servicio_cod = servicio.get('cod', None)
                        servicio_destino = servicio.get('destino', None)
                        servicio_orden = servicio.get('orden', None)
                        servicio_color = servicio.get('color', None)
                        empresa_nombre = servicio.get('negocio', {}).get('nombre', None)
                        empresa_color = servicio.get('negocio', {}).get('color', None)
                        recorrido_destino = servicio.get('recorrido', {}).get('destino', None)
                        itinerario = servicio.get('itinerario', None)
                        codigo_servicio = servicio.get('codigo', None)

                        yield {
                            'id': id_value,
                            'tipoDia': tipoDia,
                            'inicio': inicio,
                            'fin': fin,
                            'paradero_id': paradero_id,
                            'paradero_cod': paradero_cod,
                            'paradero_num': paradero_num,
                            'paradero_name': paradero_name,
                            'paradero_comuna': paradero_comuna,
                            'paradero_latitud': paradero_latitud,
                            'paradero_longitud': paradero_longitud,
                            'servicio_id': servicio_id,
                            'servicio_cod': servicio_cod,
                            'servicio_destino': servicio_destino,
                            'servicio_orden': servicio_orden,
                            'servicio_color': servicio_color,
                            'empresa_nombre': empresa_nombre,
                            'empresa_color': empresa_color,
                            'recorrido_destino': recorrido_destino,
                            'itinerario': itinerario,
                            'codigo_servicio': codigo_servicio,
                            'timestamp': timestamp
                        }
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON: {e}")
        except Exception as e:
            logging.error(f"Unexpected error: {e}")

def run(input_path, output_table):
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True  # Modo de transmisión

    with beam.Pipeline(options=options) as p:
        (p
         | 'ReadFromGCS' >> beam.io.ReadFromText(input_path)
         | 'ExtractAndTransformData' >> beam.ParDo(ExtractAndTransformData())
         | 'WriteToBigQuery' >> WriteToBigQuery(
             output_table,
             schema=table_schema,
             write_disposition=beam.io.gcp.bigquery.BigQueryDisposition.WRITE_APPEND
         ))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', required=True, help='Path of the input file in GCS')
    parser.add_argument('--output_table', required=True, help='BigQuery table to write results to')
    args, pipeline_args = parser.parse_known_args()
    
    run(args.input_path, args.output_table)
