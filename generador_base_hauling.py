import pandas as pd
import pyodbc
import numpy as np

# FUNCIONES
def check_distance_by_category_return_sums(row):
    distance_by_category = row['distance_by_category']
    
    dist_plano = 0
    dist_gt_4 = 0
    dist_lt_minus_4 = 0
    
    for nums in distance_by_category:
        as_list = nums.split(',')
        as_floats = list(map(lambda x: float(x), as_list))
        
        if -4 <= as_floats[1] <= 4:
            dist_plano += as_floats[2]
        elif as_floats[1] > 4:
            dist_gt_4 += as_floats[2]
        elif as_floats[1] < -4:
            dist_lt_minus_4 += as_floats[2]
            
    return pd.Series([dist_plano, dist_gt_4, dist_lt_minus_4], index=['dist_plano', 'dist_+4', 'dist_-4'])

def check_segundos_by_category_return_sums(row):
    distance_by_category = row['distance_by_category']
    
    segundos_plano = 0
    segundos_gt_4 = 0
    segundos_lt_minus_4 = 0
    
    for nums in distance_by_category:
        as_list = nums.split(',')
        as_floats = list(map(lambda x: float(x), as_list))
        
        if -4 <= as_floats[1] <= 4:
            segundos_plano += as_floats[3]
        elif as_floats[1] > 4:
            segundos_gt_4 += as_floats[3]
        elif as_floats[1] < -4:
            segundos_lt_minus_4 += as_floats[3]
            
    return pd.Series([segundos_plano, segundos_gt_4, segundos_lt_minus_4], index=['segundos_plano', 'segundos_+4', 'segundos_-4'])

# Conexión a SQL Server mediante ODBC:
conn_string = '''
    Driver={SQL Server}; 
    Server=192.168.200.31; 
    Database=jmineops;
    UID=jigsawdata;
    PWD=jigsawdata2@16$;
    Trusted_Connection=No;
    '''
conn = pyodbc.connect(conn_string)
query = '''\
select --time_arrive
	   cast([time_arrive] as datetime) as time
	  ,b.EquipmentName as Equipo
	  ,b.fleet as Equipo#1
      --,c.name as material_type
	  ,case when c.name = 'Estéril' then 'Estéril' when c.name = 'Vacío' then 'Vacío' else 'Mineral' end as Material
	  ,d.name as Origen
      ,e.name as Destino
      ,[expected_time]
      ,[distance]
	  ,distance_by_category
	  --,(select avg(t.tramo1_pend) from(select cast(isNULL((SELECT value FROM dbo.fn_DelimitToArray(distance_by_category,',') WHERE pos=2),0) as int) as tramo1_pend from [jmineops].[dbo].[shift_hauls]) t) as calis
  from [jmineops].[dbo].[shift_hauls] a
  left join jmineops.BI.dsvReasons_rEquipment_dim b
  on a.[equipment_id] = b.id
  left join jmineops.dbo.enum_tables c
  on a.[material_id] = c.id
  left join [jmineops].[dbo].[locations] d
  on a.[start_location_id] = d.id
  left join [jmineops].[dbo].[locations] e
  on a.[end_location_id] = e.id
  where a.[distance_by_category] is not null
  and year([time_arrive]) >= 2016
   '''
df_crudo = pd.read_sql(query,conn)

# df_crudo = "hauling_temp_csv.csv"
# FILENAME = 'hauling_temporal_csv.csv'
# df_crudo.to_csv(FILENAME, sep=',', encoding='utf-8', index=False)

casting = {
    'time': 'datetime64[ns]',
}

df = df_crudo.astype(casting)

splitted = df['distance_by_category'].str.split('\),\(')
splitted = splitted.apply(lambda x: list(map(lambda y: y.replace('(', '').replace(')', ''), x)))

df['distance_by_category'] = splitted

df[['dist_plano', 'dist_+4', 'dist_-4']] = df.apply(check_distance_by_category_return_sums, axis=1)
df[['segundos_plano', 'segundos_+4', 'segundos_-4']] = df.apply(check_segundos_by_category_return_sums, axis=1)

df['kmh_plano'] = (df['dist_plano']/df['segundos_plano'])*3.6
df['kmh_+4'] = (df['dist_+4']/df['segundos_+4'])*3.6
df['kmh_-4'] = (df['dist_-4']/df['segundos_-4'])*3.6
df['kmh_total'] = (df['distance']/(df['segundos_plano']+df['segundos_+4']+df['segundos_-4']))*3.6

df['tipo_ciclo'] = np.where(df.Material == 'Vacío', 'Viajando', 'Acarreando')

df['año'] = df['time'].dt.year

hauling = df.drop(columns='distance_by_category')

file_name = 'hauling_historico_csv.csv'

df.to_csv(file_name, sep=',', encoding='utf-8', index=False)

