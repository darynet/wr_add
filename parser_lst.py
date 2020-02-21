## -*- coding: utf-8 -*-

import psycopg2
import pandas as pd
import glob, os
import tkinter as tk
from datetime import datetime
from tkinter import messagebox as mb
import sys

path = os.getcwd()	# получим путь к директории, где лежит скрипт
path_in = path + r'\tmp' # путь, где лежат эксельки для загрузки в workregistry
path_out = path + r'\2020\Февраль' # путь, куда выгружаются эксельки для СУТЗ

# Импорт данных в workregistry. Спарсим данные из экселек, подключимся к БД и запустим скрипт загрузки
def impExcel ():
	all_files = glob.glob(path_in + "\*.xlsx")
	insertdata = []
	dataset = []
	fields = ['Дата','Исполнитель',	'Код', 'Наименование', 'Работы', 'Список контактов по работе', 'Затрачено времени (в минутах)', 'Видимость', 'Код задачи', 'Краткое наименование задачи', 'Контрагент', 'Вид затрат', 'Функциональный блок', 'Вид работ', 'Вид услуг СФ',	'Вид формирования СФ', 'Состояние', 'Закрытых заявок Ремеди']
	try:
		for filename in all_files:
			tz = pd.read_excel(filename, sheet_names='Первичное внесение данных', usecols=fields)
			tz = tz.fillna('')
			tz.loc[tz['Закрытых заявок Ремеди'] == '', 'Закрытых заявок Ремеди'] = '0'	# ClosedRemedyRequestCnt - int, пустое или null при записи в БД не прокатывает
			tz['Дата'] = tz['Дата'].apply(str)	#чтоб дата нормально отображалась
			for row in tz.itertuples(index=True, name=None):	# это видимо прописывают индексы к каждой строчке (?)
				if row[1] != '' or row[2] != '' or row[3] != '': #берем только те строки, по которым указана дата, ФИО и таск
					dataset.append(row)
	
	# очень тупо меняем индексы (потому что нормально через цикл у меня не получилось сделать)
		i=0
		for row in dataset:
			row = list(row)
			row[0] = i+1
			i=i+1
			insertdata.append(row)

	# коннектимся к БД
		conn = psycopg2.connect(host='localhost', dbname='WorkRegistry', user='testuser', password='1')
		cursor = conn.cursor()
		error_date = pd.DataFrame(columns=['Индекс','Дата','Исполнитель','Код', 'Наименование', 'Работы', 'Список контактов по работе', 'Затрачено времени (в минутах)', 'Видимость', 'Код задачи', 'Краткое наименование задачи', 'Контрагент', 'Вид затрат', 'Функциональный блок', 'Вид работ', 'Вид услуг СФ',	'Вид формирования СФ', 'Состояние', 'Закрытых заявок Ремеди'])
		msg = []
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
			  if is_table_temporary('tmp__res_post') <= 0 then
			    create temp table tmp__res_post(mtype int, minfo varchar, mdescr varchar);
			    RAISE NOTICE 'Таблица tmp__res_post создана';
			  else
			    truncate table tmp__res_post;
			    RAISE NOTICE 'Таблица tmp__res_post уже сужествует, очищена';
			  end if;
			 
			  select tzi__init() into r_init;
			  if r_init <> 999 /* успешное завершение */ then
			    raise exception 'Ошибка инициализации %%s', r_init;
			  end if;

			  insert into Data4Registry (rown, WorkDT, Executor, WorkCode, WorkName, WorkDescr, Party, TimeCosts, VisibilityName, ObjectiveCode, ObjectiveName, CustomerInfo, CostKindInfo, FunctionBlockInfo, WorkKindInfo, SFServKindInfo, SFMakeKindInfo, StateInfo, ClosedRemedyRequestCnt)
			values (%s, %s,%s,%s,%s,%s,%s,%s,%s,NULLIF(%s,''),NULLIF(%s,''),%s,%s,%s,%s,%s,%s,%s,%s)
			;

		  insert into tmp__res_post (mtype, minfo, mdescr)
		    select mtype, minfo, mdescr
		      from tzi__post()
		  ;

		  if not exists(select * from tmp__res_post where mtype = 999 /* успешное завершение */) then
		    raise INFO 'Ошибка при записи данных %%', r_post;
		  else
		    select id_portion, first_reg_id, cnt_reg from DataPostInfo into v__id_portion, v__first_reg_id, v__cnt_reg;
		    RAISE INFO 'id_portion = %%, first_reg_id = %%, cnt_reg = %%', v__id_portion, v__first_reg_id, v__cnt_reg;
		  end if;

		end;
		$body$ language plpgsql;
		select * from tmp__res_post;
		
			""", insertdata[row])
			cursor.execute("select * from tmp__res_post;")
			tmp = cursor.fetchall()
			if tmp[0][0] != 999:
				msg.append(str(tmp) + str(insertdata[row]))
				if error_date.size == 0: i = 0
				else:i = max(error_date.index) + 1
				error_date.loc[i] = insertdata[row]
		#cursor.close() 
		conn.commit()
		# запрос выполняется к каждой спарсенной строчке последовательно, а не ко всему объему данных (оставлю это здесь, а то жзабуду через пять минут). Т.е. в "values (%s, %s..." подставляется одна единственная строка.
		# вывод ошибок стремный, конечно. надо добавить переменную, в которую записывать саму спарсенную строку, на которой возникает ошибка, а не только то, что выводит хранимка Евгения
		conn.close()	# Закрываем подключение (rollback)
		if msg != []:
			mb.showinfo("Result", msg)
		else: mb.showinfo("Result", 'успешное завершение')	#сообщим что, загрузили что-то в БД
	except ValueError:
			mb.showerror("Ошибка", sys.exc_info()[1])	# расскажем, что пошло не так

# получение дат для экспорта в СУТЗ 
def getDTB():
	global dateB
	dateB = "'" + DTB.get() + "'"
	dateB = dateB.strip("\'")
	DTB_view.set(dateB)

def getDTE():
	global dateE
	dateE = "'" + DTE.get() + "'"
	dateE = dateE.strip("\'")
	DTE_view.set(dateE)

# получение пути, куда выгружаются эксельки для СУТЗ
# перетащи path_out сюда

# экспорт в СУТЗ: подключение к БД, выполнение скрипта Export2SUTZ, запись в эксель
def getExcel ():
	# department = 3	# 1 - Управление (Овсянкин Е.), 2 - Отдел разработки, 3 - Отдел сопровождения
	# dateB = '2019-06-03'
	# dateE = '2019-06-09'

	# криво, но когда-нибудь я это исправлю
	if dep_UPBS.get() == 1:
		department = 1
	elif dep_Developers.get() == 1:
		department = 2
	elif dep_Support.get() == 1:
		department = 3
	try:
		# headers = ['ТН', 'ФИО', 'Код ОА', 'IID УКС/Проекта', 'Описание работ', 'ПН', 'ВТ', 'СР', 'ЧТ', 'ПТ', 'СБ', 'ВС']
		conn = psycopg2.connect(host='localhost', dbname='WorkRegistry', user='testuser', password='1')
		cursor = conn.cursor()
		cursor.execute('SELECT * FROM  Export2SUTZ(%s::date,%s::date, %s::int)', (dateB, dateE, department))
		records = cursor.fetchall()
		dateB_format = datetime(int(dateB[0:4]), int(dateB[5:7]), int(dateB[8:10]))	# зачеем?? боже зачееем?? можно же так - datetime.datetime.strptime(dateB,'%Y-%m-%d') #и вообще перенести сразу в функцию
		dateE_format = datetime(int(dateE[0:4]), int(dateE[5:7]), int(dateE[8:10]))	# datetime.datetime.strptime(dateE,'%Y-%m-%d')
		if department == 1:
			records.insert(0, ('', '', '', '', '','', '', '', '', '', '', ''))		
			records.insert(0, ('ТН', 'ФИО', 'Код ОА', 'IID УКС/Проекта', 'Описание работ', 'ПН', 'ВТ', 'СР', 'ЧТ', 'ПТ', 'СБ', 'ВС'))
			records.insert(0, ('Период регистрации с: {} по: {}'.format(dateB_format.strftime("%d.%m.%Y"), dateE_format.strftime("%d.%m.%Y")), '', '', '', '','', '', '', '', '', '', ''))
			records.insert(0, ('МП00-0239 *код подразделения', '', '', '', '','', '', '', '', '', '', ''))
			records.insert(0, ('Управление производственных и бизнес-систем', '', '', '', '','', '', '', '', '', '', ''))											
			result = pd.DataFrame(records)
			result.to_excel(path_out + "\Шаблон_СУТЗ_{}-{}.xlsx".format(dateB, dateE), startrow=0, index=False, header=None)
		if department == 2:
			records.insert(0, ('', '', '', '', '','', '', '', '', '', '', ''))		
			records.insert(0, ('ТН', 'ФИО', 'Код ОА', 'IID УКС/Проекта', 'Описание работ', 'ПН', 'ВТ', 'СР', 'ЧТ', 'ПТ', 'СБ', 'ВС'))
			records.insert(0, ('Период регистрации с: {} по: {}'.format(dateB_format.strftime("%d.%m.%Y"), dateE_format.strftime("%d.%m.%Y")), '', '', '', '','', '', '', '', '', '', ''))
			records.insert(0, ('МП00-0240 *код подразделения', '', '', '', '','', '', '', '', '', '', ''))
			records.insert(0, ('Отдел разработки ПО', '', '', '', '','', '', '', '', '', '', ''))											
			result = pd.DataFrame(records)
			result.to_excel(path_out + "\Шаблон_СУТЗ_{}-{}_разраб.xlsx".format(dateB, dateE), startrow=0, index=False, header=None)
		if department == 3:
			records.insert(0, ('', '', '', '', '','', '', '', '', '', '', ''))		
			records.insert(0, ('ТН', 'ФИО', 'Код ОА', 'IID УКС/Проекта', 'Описание работ', 'ПН', 'ВТ', 'СР', 'ЧТ', 'ПТ', 'СБ', 'ВС'))
			records.insert(0, ('Период регистрации с: {} по: {}'.format(dateB_format.strftime("%d.%m.%Y"), dateE_format.strftime("%d.%m.%Y")), '', '', '', '','', '', '', '', '', '', ''))
			records.insert(0, ('МП00-0241 *код подразделения', '', '', '', '','', '', '', '', '', '', ''))
			records.insert(0, ('Отдел внедрения и сопровождения ПО', '', '', '', '','', '', '', '', '', '', ''))									
			result = pd.DataFrame(records)
			result.to_excel(path_out + "\Шаблон_СУТЗ_{}-{}_сопр.xlsx".format(dateB, dateE), startrow=0, index=False, header=None)
		# print(records)
		cursor.close()
		conn.close()
		mb.showinfo("Успех", "Вроде збс")
	except NameError:
		error = sys.exc_info()
		mb.showerror("Ошибка", sys.exc_info()[1])

def getDTBV():
	global dateBV
	dateBV = "'" + DTBV.get() + "'"
	dateBV = dateBV.strip("\'")
	DTBV_view.set(dateBV)


def getDTEV():
	global dateEV
	dateEV = "'" + DTEV.get() + "'"
	dateEV = dateEV.strip("\'")
	DTEV_view.set(dateEV)

def impVacation():
	# dateB_Vacation = '2019-07-08'
	# dateE_Vacation = '2019-07-12'
	# name = 'Палагин'
	# info = 'Отпуск'
	conn = psycopg2.connect(host='localhost', dbname='WorkRegistry', user='testuser', password='1')
	cursor = conn.cursor()
	cursor.execute('SELECT * FROM tzi_DaysOff(%s::date, %s::date, %s, %s)', (dateBV, dateEV, name, info))
	err = cursor.fetchall()
	if err[0][0] == -1:
		mb.showerror("Ошибка", 'Не удалось определить сотрудника.')
	if err[0][0] == -2:
		mb.showerror("Ошибка", 'Указаны некорректные даты.')
	if err[0][0] == 999:
		mb.showinfo("Успех", "Вроде збс")
	cursor.close()
	# conn.close()
	conn.commit()

def getName():
	global name
	name = "'" + name_empl.get() + "'"
	name = name.strip("\'")
	name_view.set(name)

def getInfo():
	global info
	info = "'" + info_str.get() + "'"
	info = info.strip("\'")
	info_view.set(info)

# кнопочки для Саши
root = tk.Tk()
FirstFrame = tk.Frame(root, width = 360, height = 380)
SecondFrame = tk.Frame(root, width = 345, height = 380)
ImportFrame = tk.Frame(FirstFrame, width = 360, height = 100)
ExportFrame = tk.Frame(FirstFrame, width = 360, height = 280, bg = 'lightsteelblue')
VacationFrame = tk.Frame(SecondFrame, width = 345, height = 380, bg = 'lightgrey')

# FirstFrame.pack()
# SecondFrame.pack()
FirstFrame.grid(row = 1, column = 1)
SecondFrame.grid(row = 1, column = 2)
ImportFrame.grid(row = 1, column = 1)
ExportFrame.grid(row = 2, column = 1)
VacationFrame.grid(row = 1, column = 1)

# кнопка для импорта данных в workregistry
browseButton_Import = tk.Button(ImportFrame, text='Загрузка в WorkRegistry', command=impExcel, fg='black', font=('helvetica', 12, 'bold'))
browseButton_Import.place(x = 30, y = 10, width = 300, height = 75)

# кнопки для экспорта в СУТЗ
DTB = tk.StringVar()
dateBegin = tk.Entry(ExportFrame, textvariable=DTB)
dateBegin.place(x = 120, y = 10, width = 100, height = 25)
dateBegin_button = tk.Button(ExportFrame, text="Начало периода", command=getDTB)
dateBegin_button.place(x = 230, y = 10, width = 115, height = 25)

DTB_view = tk.StringVar()
DTB_label = tk.Label(ExportFrame, textvariable=DTB_view, fg="#eee", bg="#333", padx=0)
DTB_label.place(x = 10, y = 10, width = 100, height = 25)

DTE = tk.StringVar()
dateEnd = tk.Entry(ExportFrame, textvariable=DTE)
dateEnd.place(x = 120, y = 50, width = 100, height = 25)
dateEnd_button = tk.Button(ExportFrame, text="Окончание периода", command=getDTE)
dateEnd_button.place(x = 230, y = 50, width = 115, height = 25)

DTE_view = tk.StringVar()
DTE_label = tk.Label(ExportFrame, textvariable=DTE_view, fg="#eee", bg="#333", padx=0)
DTE_label.place(x = 10, y = 50, width = 100, height = 25)

# Отделы (для выгрузки в СУТЗ)
dep_UPBS = tk.IntVar()
dep_UPBS_checkbutton = tk.Checkbutton(ExportFrame, text="УПБС", variable=dep_UPBS,
                                 onvalue=1, offvalue=0, padx=10, pady=10, bg = 'lightsteelblue')
dep_Developers = tk.IntVar()
dep_Developers_checkbutton = tk.Checkbutton(ExportFrame, text="Отдел разработки ПО", variable=dep_Developers,
                                     onvalue=1, offvalue=0, padx=10, pady=10, bg = 'lightsteelblue')
dep_Support = tk.IntVar()
dep_Support_checkbutton = tk.Checkbutton(ExportFrame, text="Отдел внедрения и сопровождения ПО", variable=dep_Support,
                                     onvalue=1, offvalue=0, padx=10, pady=10, bg = 'lightsteelblue')
dep_UPBS_checkbutton.place(x = 10, y = 90)
dep_Developers_checkbutton.place(x = 100, y = 90)
dep_Support_checkbutton.place(x = 10, y = 130)

browseButton_Export = tk.Button(ExportFrame, text='Выгрузка для СУТЗ', command=getExcel, fg='black', font=('helvetica', 12, 'bold'))
browseButton_Export.place(x = 30, y = 180, width = 300, height = 75)

# кнопки для вакэйшна
DTBV = tk.StringVar()
dateBeginV = tk.Entry(VacationFrame, textvariable=DTBV)
dateBeginV.place(x = 120, y = 10, width = 100, height = 25)
dateBeginV_button = tk.Button(VacationFrame, text="Дата начала", command=getDTBV)
dateBeginV_button.place(x = 230, y = 10, width = 100, height = 25)

DTBV_view = tk.StringVar()
DTBV_label = tk.Label(VacationFrame, textvariable=DTBV_view, fg="#eee", bg="#333", padx=0)
DTBV_label.place(x = 10, y = 10, width = 100, height = 25)

DTEV = tk.StringVar()
dateEndV = tk.Entry(VacationFrame, textvariable=DTEV)
dateEndV.place(x = 120, y = 45, width = 100, height = 25)
dateEndV_button = tk.Button(VacationFrame, text="Дата окончания", command=getDTEV)
dateEndV_button.place(x = 230, y = 45, width = 100, height = 25)

DTEV_view = tk.StringVar()
DTEV_label = tk.Label(VacationFrame, textvariable=DTEV_view, fg="#eee", bg="#333", padx=0)
DTEV_label.place(x = 10, y = 45, width = 100, height = 25)

name_empl = tk.StringVar()
name = tk.Entry(VacationFrame, textvariable=name_empl)
name.place(x = 120, y = 110, width = 100, height = 25)
name_button = tk.Button(VacationFrame, text="Сотрудник", command=getName)
name_button.place(x = 230, y = 110, width = 100, height = 25)

name_view = tk.StringVar()
name_label = tk.Label(VacationFrame, textvariable=name_view, fg="#eee", bg="#333", padx=0)
name_label.place(x = 10, y = 110, width = 100, height = 25)

info_str = tk.StringVar()
info = tk.Entry(VacationFrame, textvariable=info_str)
info.place(x = 120, y = 140, width = 100, height = 25)
info_button = tk.Button(VacationFrame, text="Комментарий", command=getInfo)
info_button.place(x = 230, y = 140, width = 100, height = 25)

info_view = tk.StringVar()
info_label = tk.Label(VacationFrame, textvariable=info_view, fg="#eee", bg="#333", padx=0)
info_label.place(x = 10, y = 140, width = 100, height = 25)

browseButton_Vacation = tk.Button(VacationFrame, text='Добавить отпуск', command=impVacation, fg='black', font=('helvetica', 12, 'bold'))
browseButton_Vacation.place(x = 23, y = 190, width = 300, height = 75)

root.mainloop()
