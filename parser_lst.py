import psycopg2
import pandas as pd
import glob, os

path = r'D:\Project\WorkRegistry\dataXLS\tmp' # путь где лежат эксельки
all_files = glob.glob(path + "\*.xlsx")

insertdata = []
dataset = []
fields = ['Дата','Исполнитель',	'Код', 'Наименование', 'Работы', 'Список контактов по работе', 'Затрачено времени (в минутах)', 'Состояние', 'Видимость', 'Закрытых заявок Ремеди', 'Контрагент', 'Вид затрат', 'Функциональный блок', 'Вид работ', 'Вид услуг СФ',	'Вид формирования СФ']
for filename in all_files:
    tz = pd.read_excel(filename, sheet_names='Первичное внесение данных', usecols=fields)
    tz = tz.fillna('')
    tz.loc[tz['Закрытых заявок Ремеди'] == '', 'Закрытых заявок Ремеди'] = '0'	# ClosedRemedyRequestCnt - int, пустое или null при записи в БД не прокатывает
    tz['Дата'] = tz['Дата'].apply(str)	#

    for row in tz.itertuples(index=True, name=None):
        dataset.append(row)

# очень тупо меняем индексы (потому что нормально через цикл у меня не получилось сделать)
i=0
for row in dataset:
	row = list(row)
	row[0] = i+1
	i=i+1
	insertdata.append(row)
# print(insertdata)
# коннектимся к БД
conn = psycopg2.connect(host='localhost', dbname='work_registry', user='testuser', password='1')
cursor = conn.cursor()
 
# insert = sql.SQL(
for row in range(len(insertdata)):
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
	""", insertdata[row])

cursor.close() # Закрываем подключение (rollback)
# conn.commit()
conn.close()
