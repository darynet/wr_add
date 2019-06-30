#!/usr/bin/python
# -*- coding: iso-8859-15 -*-

# import psycopg2
import pandas as pd
import glob, os
import Tkinter as tk


root= tk.Tk()

panelFrame = tk.Frame(root, width = 800, height = 500, bg = 'lightsteelblue')
panelFrame.pack()

path = os.getcwd()

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
	dateB = "'" + DTB.get() + "'"
	DTB_view.set(dateB)


def getDTE():
	global dateE
	dateE = "'" + DTE.get() + "'"
	DTE_view.set(dateE)

DTB = tk.StringVar()
DTE = tk.StringVar()

dateBegin = tk.Entry(panelFrame, textvariable=DTB)
dateEnd = tk.Entry(panelFrame, textvariable=DTE)
dateBegin_button = tk.Button(panelFrame, text="Set DateB", command=getDTB)
dateEnd_button = tk.Button(panelFrame, text="Set DateE", command=getDTE)
# dateBegin_label = tk.Label(panelFrame, text="DateB:")

dateBegin_button.place(x = 230, y = 10, width = 90, height = 25)
dateEnd_button.place(x = 230, y = 45, width = 90, height = 25)
dateBegin.place(x = 120, y = 10, width = 100, height = 25)
dateEnd.place(x = 120, y = 45, width = 100, height = 25)
# dateBegin_label.place(x = 10, y = 10, width = 60, height = 25)

DTB_view = tk.StringVar()
DTB_label = tk.Message(panelFrame, textvariable=DTB_view)
DTB_label.place(x = 10, y = 10, width = 100, height = 25)

DTE_view = tk.StringVar()
DTE_label = tk.Message(panelFrame, textvariable=DTE_view)
DTE_label.place(x = 10, y = 45, width = 100, height = 25)


dep_UPBS = tk.IntVar()
dep_UPBS_checkbutton = tk.Checkbutton(panelFrame, text="УПБС", variable=dep_UPBS,
                                 onvalue=1, offvalue=0, padx=15, pady=10)
 
dep_Developers = tk.IntVar()
dep_Developers_checkbutton = tk.Checkbutton(panelFrame, text="Отдел разработки ПО", variable=dep_Developers,
                                     onvalue=1, offvalue=0, padx=15, pady=10)
dep_Support = tk.IntVar()
dep_Support_checkbutton = tk.Checkbutton(panelFrame, text="Отдел внедрения и сопровождения ПО", variable=dep_Support,
                                     onvalue=1, offvalue=0, padx=15, pady=10)
dep_UPBS_checkbutton.place(x = 10, y = 300)
dep_Developers_checkbutton.place(x = 100, y = 300)
dep_Support_checkbutton.place(x = 280, y = 300)

def getExcel ():
	# department = 3	# 1 - Управление (Овсянкин Е.), 2 - Отдел разработки, 3 - Отдел сопровождения
	# dateB = '2019-06-03'
	# dateE = '2019-06-09'
	if dep_UPBS.get() == 1:
		department = 1
	elif dep_Developers.get() == 1:
		department = 2
	elif dep_Support.get() == 1:
		department = 3
	headers = ['ТН', 'ФИО', 'Код ОА', 'IID УКС/Проекта', 'Описание работ', 'ПН', 'ВТ', 'СР', 'ЧТ', 'ПТ', 'СБ', 'ВС']

	conn = psycopg2.connect(host='localhost', dbname='work_registry', user='testuser', password='1')
	cursor = conn.cursor()
	cursor.execute('SELECT * FROM  Export2SUTZ(%s::date,%s::date, %s::int)', (dateB, dateE, department))
	records = cursor.fetchall()
	result = pd.DataFrame(records)
	if department == 1:
		result.to_excel(path + "\Шаблон_СУТЗ_.xlsx", startrow=4, index=False, header=headers)
	if department == 2:
		result.to_excel(r'D:\Project\WorkRegistry\dataXLS\tmp\SUTZ\Шаблон_СУТЗ_разраб.xlsx', startrow=4, index=False, header=headers)
	if department == 3:
		result.to_excel(r'D:\Project\WorkRegistry\dataXLS\tmp\SUTZ\Шаблон_СУТЗ_сопр.xlsx', startrow=4, index=False, header=headers)
	# print(records)
	cursor.close()
	conn.close()

browseButton_Excel = tk.Button(panelFrame, text='Import Data to WorkRegistry', command=impExcel, bg='green', fg='white', font=('helvetica', 12, 'bold'))
browseButton_Export = tk.Button(panelFrame, text='Export Data for SUTZ', command=getExcel, bg='green', fg='white', font=('helvetica', 12, 'bold'))

browseButton_Excel.place(x = 10, y = 100, width = 300, height = 75)
browseButton_Export.place(x = 10, y = 180, width = 300, height = 75)
root.mainloop()
