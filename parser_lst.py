##Connect to postgres (test db)
import psycopg2
from psycopg2 import sql
import pandas as pd

df = pd.read_excel('test.xlsx', sheet_names='Лист1')
df = df.fillna('')

dataset = []
df.index = range(1,len(df)+1)
df.loc[df['Закрытых заявок Ремеди'] == '', 'Закрытых заявок Ремеди'] = '0'
df['Дата'] = df['Дата'].apply(str)

for row in df.itertuples(index=True, name=None):	
	dataset.append(row)


conn = psycopg2.connect(host='localhost', dbname='work_registry', user='testuser', password='1')
cursor = conn.cursor()
 
# insert = sql.SQL(
for row in range(len(dataset)):
	cursor.execute(
	   	"""do $body$
	declare
	  r_init int;
	  r_post int;
	  v__cnt_reg int;
	  v__id_portion int;
	  v__first_reg_id int;
	begin
	  select tzi__init() into r_init;
	  if r_init <> 999 /* успешное завершение */ then
	    raise exception 'Ошибка инициализации %%s', r_init;
	  end if;

	  insert into Data4Registry (rown, WorkDT, Executor, WorkCode, WorkName, WorkDescr, Party, TimeCosts, StateInfo, VisibilityName, ClosedRemedyRequestCnt, CostKindInfo, CustomerInfo, FunctionBlockInfo, WorkKindInfo, SFServKindInfo, SFMakeKindInfo)
	values (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
	;

  select tzi__post() into r_post;
  if r_post <> 999 /* успешное завершение */ then
    raise exception 'Ошибка при записи данных %%', r_post;
  else
    select id_portion, first_reg_id, cnt_reg from DataPostInfo into v__id_portion, v__first_reg_id, v__cnt_reg;
    RAISE INFO 'id_portion = %%, first_reg_id = %%, cnt_reg = %%', v__id_portion, v__first_reg_id, v__cnt_reg;
  end if;

	end;
	$body$ language plpgsql;
	""", dataset[row])
# .format(
#         sql.SQL(',').join(map(sql.Literal, dataset)))

# cursor.executemany(insert, QueryData)
# cursor.execute('Select * from Registry where id_executor=11 and id_task=122')
# row = cursor.fetchall()
# print (row)

# Закрываем подключение.
cursor.close()
# conn.commit()
conn.close()