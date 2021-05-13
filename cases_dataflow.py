
import datetime, logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

class FormatDate(beam.DoFn):
  def process(self, element):
    combined_key = element['combined_key']
    day_key = element['day_key']
    confirmed = element['confirmed']
    deaths = element['deaths']
    recovered = element['recovered']

    # split using comma to get [city,state,date]
    city,state,date = day_key.split(',')
    
    # Remove whitesapce in city name
    if len(city.split(' '))!=1 :
        city=city.replace(' ','_')
        
    # Remove whitesapce in city name
    if len(state.split(' '))!=1 :
        state=state.replace(' ','_')
        
    # Standardize date (mm/dd/yy)
    
    # Split key by whitespace(removes time), e.g Iowa,Ida,3/22/20 23:45  >Iowa,Ida,3/22/20
    date = date.split(' ')[0]
    
    # determine if date is in m/dd/yy or yyyy-dd-mm format
    if len(date.split('/'))==1:
        # date is in yyyy-mm-dd format
        year,month,day=date.split('-')
        year=year[2:]
        
    else:
        # date is in m/dd/yy
        month,day,year=date.split('/')
        if len(day)!=2:
            day='0'+day
        if len(month)!=2:
            month='0'+month
    date=month+'/'+day+'/'+year
   
    # combine to make keys
    combined_key=state+'_'+city
    day_key=combined_key+'_'+date
    
    record = {'combined_key': combined_key, 'day_key': day_key, 'confirmed': confirmed, 'deaths': deaths, 'recovered': recovered}
    return [record]   
        
def run():
    PROJECT_ID = 'starlit-vim-303003'
    BUCKET = 'gs://pandas-aj/temp'

    DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'
    
    options = PipelineOptions(
    flags=None,
    runner='DataflowRunner',
    project=PROJECT_ID,
    job_name='day',
    temp_location=BUCKET + '/temp',
    region='us-central1')
        
    p = beam.pipeline.Pipeline(options=options)

    sql = 'SELECT * FROM datamart.Cases'
    bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

    query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)

    out_pcoll = query_results | 'Format Name' >> beam.ParDo(FormatDate())

    out_pcoll | 'Write Results' >> WriteToText(DIR_PATH + 'output_cases.txt')

    dataset_id = 'datamart'
    table_id = PROJECT_ID + ':' + dataset_id + '.' + 'cases_Dataflow'
    schema_id = 'combined_key:STRING,day_key:STRING,confirmed:INTEGER,deaths:INTEGER,recovered:INTEGER'

    out_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
     
    result = p.run()
    result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()