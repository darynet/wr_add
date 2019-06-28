import psycopg2
import pandas as pd
import glob, os
import tkinter as tk
from tkinter import filedialog

root= tk.Tk()

panelFrame = tk.Frame(root, width = 500, height = 500, bg = 'lightsteelblue')
panelFrame.pack()

path = r'D:\Project\WorkRegistry\dataXLS\tmp' # путь где лежат эксельки

def impExcel ():
	all_files = glob.glob(path + "\*.xlsx")

	insertdata = []
	dataset = []
	fields = ['Дата','Исполнитель',	'Код', 'Наименование', 'Работы', 'Список контактов по работе', 'Затрачено времени (в минутах)', 'Состояние', 'Видимость', 'Закрытых заявок Ремеди', 'Контрагент', 'Вид затрат', 'Функциональный блок', 'Вид работ', 'Вид услуг СФ',	'Вид формирования СФ']
	for filename in all_files:
		tz = pd.read_excel(filename, sheet_names='Первичное внесение данных', usecols=fields)
		# tz.dropna(axis=0, how='all', thresh=None, subset=None, inplace=False)
		tz = tz.fillna('')
		tz.loc[tz['Закрытых заявок Ремеди'] == '', 'Закрытых заявок Ремеди'] = '0'	# ClosedRemedyRequestCnt - int, пустое или null при записи в БД не прокатывает
		tz['Дата'] = tz['Дата'].apply(str)	#
		for row in tz.itertuples(index=True, name=None):
			if row[1] != '' and row[2] != '' and row[3] != '':
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

	cursor.close() 
	# conn.commit()
	conn.close()	# Закрываем подключение (rollback)

def getDTB():
	global dateB
	dateB = "'" + dateB.get() + "'"


def getDTE():
	global dateE
	dateE = "'" + dateE.get() + "'"
	

dateB = tk.StringVar()
dateE = tk.StringVar()

dateBegin = tk.Entry(panelFrame, textvariable=dateB)
dateEnd = tk.Entry(panelFrame, textvariable=dateE)
dateBegin_button = tk.Button(text="Set DateB", command=getDTB)
dateEnd_button = tk.Button(text="Set DateE", command=getDTE)

dateBegin_button.place(x = 130, y = 10, width = 90, height = 25)
dateEnd_button.place(x = 130, y = 45, width = 90, height = 25)
dateBegin.place(x = 10, y = 10, width = 100, height = 25)
dateEnd.place(x = 10, y = 45, width = 100, height = 25)


def getExcel ():
	department = 3	# 1 - Управление (Овсянкин Е.), 2 - Отдел разработки, 3 - Отдел сопровождения
	# dateB = '2019-06-03'
	# dateE = '2019-06-09'
	headers = ['ТН', 'ФИО', 'Код ОА', 'IID УКС/Проекта', 'Описание работ', 'ПН', 'ВТ', 'СР', 'ЧТ', 'ПТ', 'СБ', 'ВС']

	conn = psycopg2.connect(host='localhost', dbname='work_registry', user='testuser', password='1')
	cursor = conn.cursor()
	cursor.execute('SELECT * FROM  Export2SUTZ(%s::date,%s::date, %s)', (dateB, dateE, department))
	records = cursor.fetchall()
	result = pd.DataFrame(records)
	if department == 1:
		result.to_excel(path + "\SUTZ\Шаблон_СУТЗ_.xlsx", startrow=4, index=False, header=headers)
	if department == 2:
		result.to_excel(path + "\SUTZ\Шаблон_СУТЗ_разраб.xlsx", startrow=4, index=False, header=headers)
	if department == 3:
		result.to_excel(path + "\SUTZ\Шаблон_СУТЗ_сопр.xlsx", startrow=4, index=False, header=headers)
	# print(records)
	cursor.close()
	conn.close()


browseButton_Excel = tk.Button(panelFrame, text='Import Data to WorkRegistry', command=impExcel, bg='green', fg='white', font=('helvetica', 12, 'bold'))
browseButton_Export = tk.Button(panelFrame, text='Export Data for SUTZ', command=getExcel, bg='green', fg='white', font=('helvetica', 12, 'bold'))

browseButton_Excel.place(x = 10, y = 100, width = 300, height = 75)
browseButton_Export.place(x = 10, y = 180, width = 300, height = 75)
root.mainloop()
