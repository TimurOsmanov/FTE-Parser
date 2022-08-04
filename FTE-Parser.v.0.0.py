from aiogram import Bot, Dispatcher, executor, types
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher import FSMContext
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from pyrogram import Client
import datetime
import aioschedule
import asyncio
import sqlite3
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import itertools
from PyPDF2 import PdfFileReader, PdfFileWriter

# 1. Install libs: pip install aiogram pip install pyrogram pip install aioschedule pip install asyncio
# pip install matplotlib, pip install PyPDF2
# You can install SQLite to monitor changes in DB. To install it for Windows download 2 files
# from https://www.sqlite.org/download.html from Precompiled Binaries for Windows:
# first is sqlite-dll-win32-x86-3390200.zip or sqlite-dll-win64-x64-3390200.zip according to your system (32/64),
# second sqlite-tools-win32-x86-3390200.zip
# 2. Firstly you have to login in https://my.telegram.org/ to manage your apps using Telegram API.
# On the first run you'll be asked to enter your phone number or
# bot token (i use my phone) to login https://my.telegram.org/.
# You have to enter confirmation code sent to your phone.
# Now you can use app
# 3. Bot will work correctly if you create group and then add bot there

bot_token, api_hash = '', '4'
api_id, group_id, manager_group_id = 0, 0, 0
polls, projects_by_polls, questions, chosen_projects, question_index, report, workers_in_db = {}, {}, {}, {}, {}, {}, {}
polls_num = {}
projects_set_by_admin, projects_set_by_admin_div, chat_members, chat_admins = [], [], [], [0, 0]
lost_names = []
bot = Bot(bot_token)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)


# Parsing of chat members, chat members will receive polls with projects


def get_chat_members():
    with Client("my_account", api_id, api_hash) as app:
        for member in app.get_chat_members(group_id):
            if not member.user.is_bot:
                chat_members.append(member.user.id)


get_chat_members()


# 1. DB-PART.
# Working with DB will be provided by this functions:


def get_worker_daily_stat():
    # Function gets number of last record to provide it to new insert_into_db
    with sqlite3.connect('sqlite_python.db') as conn1:
        cursor1 = conn1.cursor()
        select_info = 'SELECT unique_id FROM FTE_info'
        cursor1.execute(select_info)
        number = cursor1.fetchall()
        return number[-1][0]


try:
    with sqlite3.connect('sqlite_python.db') as conn2:
        cursor2 = conn2.cursor()
        create_table = '''CREATE TABLE FTE_info (unique_id INTEGER PRIMARY KEY, date datetime, 
        project text, user_id INTEGER, hours REAL NOT NULL)'''
        cursor2.execute(create_table)
        worker_daily_stat = 0
except sqlite3.Error as error_fte_info_exists:
    # error_fte_info_exists.args ('table FTE_info already exists',) is a system message when your DB already has table
    if error_fte_info_exists.args == ('table FTE_info already exists',):
        print(f"Напоминаю, таблица уже создана {error_fte_info_exists}")
        try:
            worker_daily_stat = get_worker_daily_stat()
        except IndexError:
            # IndexError raises when table is empty
            worker_daily_stat = 0
    else:
        print("Ошибка при подключении к sqlite", error_fte_info_exists)

try:
    with sqlite3.connect('sqlite_python.db') as conn3:
        cursor3 = conn3.cursor()
        create_table2 = '''CREATE TABLE workers_info (user_id INTEGER PRIMARY KEY, full_name text)'''
        cursor3.execute(create_table2)
except sqlite3.Error as error_workers_info_exists:
    # error_workers_info_exists.args ('table workers_info already exists',)
    # is a system message when your DB already has table
    if error_workers_info_exists.args == ('table workers_info already exists',):
        print(f"Напоминаю, таблица уже создана {error_workers_info_exists}")


def get_workers_names():
    global workers_in_db
    with sqlite3.connect('sqlite_python.db') as conn4:
        cursor4 = conn4.cursor()
        select_info = 'SELECT user_id, full_name FROM workers_info'
        cursor4.execute(select_info)
        names = cursor4.fetchall()
        workers_in_db = {user[0]: user[1] for user in names}


get_workers_names()


def insert_into_db(data_tuple):
    # Function inserts new record according to new answer in poll
    with sqlite3.connect('sqlite_python.db') as conn5:
        cursor5 = conn5.cursor()
        insert_info = '''INSERT INTO FTE_info (unique_id, date, project, 
        user_id, hours) VALUES (?, ?, ?, ?, ?)'''
        cursor5.execute(insert_info, data_tuple)


def check_answers():
    global report, lost_names
    get_workers_names()
    with sqlite3.connect('sqlite_python.db') as conn6:
        now = f'{datetime.datetime.now().year}-{datetime.datetime.now().month}-'f'{datetime.datetime.now().day}'
        cursor6 = conn6.cursor()
        select_info = f"SELECT user_id, SUM(hours) FROM FTE_info WHERE date = '{now}' GROUP BY user_id"
        cursor6.execute(select_info)
        daily_stat_raw = cursor6.fetchall()
        daily_stat = {user[0]: user[1] for user in daily_stat_raw}
        for worker in chat_members:
            try:
                if worker in daily_stat:
                    if not 6 <= daily_stat[worker] <= 12:
                        report[workers_in_db[worker]] = daily_stat[worker]
                else:
                    report[workers_in_db[worker]] = 'не ответил/не работал'
            except KeyError:
                # KeyError raises when worker is in chat_members (in group) but he isn't in DB
                # It could happen if he forget to provide his full name to bot
                lost_names.append(worker)


def add_new_member_to_workers_info(data_tuple):
    with sqlite3.connect('sqlite_python.db') as conn7:
        cursor7 = conn7.cursor()
        insert_info = '''INSERT INTO workers_info (user_id, full_name) VALUES (?, ?)'''
        cursor7.execute(insert_info, data_tuple)


def get_projects_names(user_id):
    with sqlite3.connect('sqlite_python.db') as conn8:
        cursor8 = conn8.cursor()
        select_info = f"SELECT project FROM FTE_info WHERE user_id = '{user_id}'"
        cursor8.execute(select_info)
        names = cursor8.fetchall()
        names = [x[0] for x in names]
        return names


def update_in_fte_info(data_tuple):
    with sqlite3.connect('sqlite_python.db') as conn9:
        cursor9 = conn9.cursor()
        insert_info = f"""UPDATE FTE_info SET hours = '{data_tuple[0]}' WHERE user_id = '{data_tuple[1]}' 
        AND date = '{data_tuple[2]}' AND project = '{data_tuple[3]}'"""
        cursor9.execute(insert_info)


def get_statistics(period):
    with sqlite3.connect('sqlite_python.db') as conn10:
        cursor10 = conn10.cursor()
        select_info = f"SELECT full_name, project, hours FROM FTE_info INNER JOIN workers_info WHERE " \
                      f"FTE_info.user_id = workers_info.user_id AND " \
                      f"date BETWEEN '{period[0]}' AND '{period[1]}' ORDER BY " \
                      f"full_name"
        cursor10.execute(select_info)
        names = cursor10.fetchall()
    today_projects = []
    for z in names:
        if z[1] not in today_projects:
            today_projects.append(z[1])
    today_worker_stat = {}
    for x in names:
        if x[0] not in today_worker_stat:
            today_worker_stat[x[0]] = {x[1]: x[2]}
        else:
            if x[1] not in today_worker_stat[x[0]]:
                today_worker_stat[x[0]][x[1]] = x[2]
            else:
                today_worker_stat[x[0]][x[1]] += x[2]
    # divide data for 20 workers in dataset
    today_worker_stat_div = []
    temp_dict, i = {}, 0
    for info in sorted(today_worker_stat):
        i += 1
        if i != len(today_worker_stat):
            if i % 18 != 0:
                temp_dict[info] = today_worker_stat[info]
            else:
                temp_dict[info] = today_worker_stat[info]
                today_worker_stat_div.append(temp_dict)
                temp_dict = {}
        else:
            temp_dict[info] = today_worker_stat[info]
            today_worker_stat_div.append(temp_dict)
            temp_dict = {}
    for_report_div = []
    for_report = {}
    for dataset in today_worker_stat_div:
        for project in today_projects:
            for_report[project] = []
            for worker in dataset:
                if project in today_worker_stat[worker]:
                    for_report[project].append(today_worker_stat[worker][project])
                else:
                    for_report[project].append(0)
        for_report_div.append(for_report)
        for_report = {}
    i1 = 0
    start_row = ' Итого'
    for dataset in today_worker_stat_div:
        for worker in dataset:
            if start_row not in for_report_div[i1]:
                for_report_div[i1][start_row] = []
                for_report_div[i1][start_row].append(round(sum(dataset[worker].values()), 2))
            else:
                for_report_div[i1][start_row].append(round(sum(dataset[worker].values()), 2))
        i1 += 1
    today_projects.append(start_row)
    return zip(itertools.repeat(today_projects), today_worker_stat_div, for_report_div)


# 2. TELEGRAM-PART.
# ADMIN-PART


class Form(StatesGroup):
    project = State()
    project_name = State()
    answer = State()
    final_answer = State()
    final_answer2 = State()
    end = State()
    first_message = State()
    message_update = State()
    get_daily_stat = State()


@dp.message_handler(commands=['set_projects'])
async def set_projects(message: types.Message):
    global chat_admins
    if message.from_user.id in chat_admins:
        await message.reply("Перечислите проекты через запятую")
        await Form.project.set()
    else:
        await bot.send_message(message.chat.id, 'Нет прав')


@dp.message_handler(state=Form.project)
async def admin_set_projects(message: types.Message, state: FSMContext):
    global projects_set_by_admin, projects_set_by_admin_div
    projects_set_by_admin, projects_set_by_admin_div = [], []
    async with state.proxy() as data:
        data['project_name'] = message.text
    projects_set_by_admin = data['project_name'].split(',')
    projects_set_by_admin = list(map(lambda x: x.strip(), projects_set_by_admin))
    temp, i = [], 0
    while i - len(projects_set_by_admin) != 0:
        temp.append(projects_set_by_admin[i])
        i += 1
        if i % 9 == 0:
            temp.append('Не участвовал в вышеперечисленных проектах')
            projects_set_by_admin_div.append(temp)
            temp = []
    if temp:
        if len(temp) == 1:
            temp.append('Не участвовал в вышеперечисленных проектах')
            projects_set_by_admin_div.append(temp)
        else:
            temp.append('Не участвовал в вышеперечисленных проектах')
            projects_set_by_admin_div.append(temp)
    await state.finish()


@dp.message_handler(commands=['send_polls'])
async def send_polls(message: types.Message):
    # Attribute of function cant be removed cause without it function raises error
    if message.from_user.id in chat_admins:
        aioschedule.every().day.at('08:00').do(create_polls)
        aioschedule.every().day.at('09:00').do(checking)
        while True:
            await aioschedule.run_pending()
            await asyncio.sleep(1)
    else:
        await bot.send_message(message.chat.id, 'Нет прав')


async def create_polls():
    global projects_set_by_admin_div, chat_members, polls, polls_num, projects_by_polls, question_index
    polls, polls_num, projects_by_polls, question_index = {}, {}, {}, {}
    for user in chat_members:
        for group_projects in projects_set_by_admin_div:
            new_poll = await bot.send_poll(user, 'В каких проектах вы принимали участие вчера?',
                                           group_projects, is_anonymous=False, allows_multiple_answers=True)
            polls[new_poll.poll.id] = group_projects


@dp.poll_answer_handler()
async def voting(call):
    global chosen_projects, worker_daily_stat, projects_by_polls, polls_num, projects_set_by_admin_div
    if not call.option_ids:
        del chosen_projects[call.user.id]
        del projects_by_polls[call.user.id]
        del polls_num[call.user.id]
        await bot.send_message(call.user.id, "Вы отозвали голос из одного из опросов.\n"
                                             "Вам необходимо проголосовать заново")
        for group_projects in projects_set_by_admin_div:
            new_poll = await bot.send_poll(call.user.id, 'В каких проектах вы принимали участие вчера?',
                                           group_projects, is_anonymous=False, allows_multiple_answers=True)
            polls[new_poll.poll.id] = group_projects
    else:
        if call.user.id not in polls_num:
            polls_num[call.user.id] = len(projects_set_by_admin_div)
        polls_num[call.user.id] -= 1
        for project_num in call.option_ids:
            if call.user.id not in projects_by_polls:
                if polls[call.poll_id][project_num] != 'Не участвовал в вышеперечисленных проектах':
                    projects_by_polls[call.user.id] = [polls[call.poll_id][project_num]]
            else:
                if polls[call.poll_id][project_num] != 'Не участвовал в вышеперечисленных проектах':
                    projects_by_polls[call.user.id].append(polls[call.poll_id][project_num])
        if polls_num[call.user.id] == 0:
            chosen_projects[call.user.id] = projects_by_polls[call.user.id]
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, selective=True)
            markup.add("Продолжить")
            await bot.send_message(call.user.id, "Нажмите продолжить", reply_markup=markup)
            await Form.final_answer.set()


@dp.message_handler(lambda message: message.text in ["Продолжить"], state=Form.final_answer)
async def create_questions_from_polls(message: types.Message, state: FSMContext):
    # Attribute of function cant be removed cause without it function raises error
    global question_index, chosen_projects
    markup_remove = types.ReplyKeyboardRemove()
    question_index[message.chat.id] = 0
    try:
        project = chosen_projects[message.chat.id][question_index[message.chat.id]]
        await bot.send_message(message.chat.id, f'Сколько часов вы были заняты на проекте {project} вчера?',
                               reply_markup=markup_remove)
        questions[message.chat.id] = project
        question_index[message.chat.id] += 1
        await Form.final_answer2.set()
    except IndexError:
        await bot.send_message(message.chat.id, f'Произошла ошибка, обратитесь к администратору')
        await state.finish()


@dp.message_handler(lambda message: message.text not in ["Продолжить"], state=Form.final_answer)
async def create_questions_from_polls(message: types.Message):
    # Attribute of function cant be removed cause without it function raises error
    await bot.send_message(message.chat.id, f"Вам нужно нажать кнопку 'Продолжить' или написать"
                                            f" Продолжить ")
    await Form.final_answer.set()


@dp.message_handler(state=Form.final_answer2)
async def create_questions_from_polls(message: types.Message, state: FSMContext):
    global question_index, worker_daily_stat, chosen_projects
    replaced_message_text = message.text.replace(',', '').replace('.', '')
    if not replaced_message_text.isdigit():
        await bot.send_message(message.chat.id, f'Ответ должен быть числом')
        await Form.final_answer2.set()
    else:
        async with state.proxy() as data:
            data['answer'] = message.text
        hours = data['answer']
        worker_daily_stat += 1
        now = f'{datetime.datetime.now().year}-{datetime.datetime.now().month}-'f'{datetime.datetime.now().day}'
        data_tuple = (worker_daily_stat, now, questions[message.chat.id], message.chat.id, hours.replace(',', '.'))
        try:
            insert_into_db(data_tuple)
        except sqlite3.OperationalError as error_final_answer2:
            # error_final_answer2.args ('database is locked',) is a system message when your DB is opened by someone
            if error_final_answer2.args == ('database is locked',):
                question_index[message.chat.id] -= 1
                await bot.send_message(message.chat.id, 'База данных используется в данный момент, попросите '
                                                        'администратора закрыть ее и отправьте ответ заново')
                await Form.final_answer2.set()
        try:
            project = chosen_projects[message.chat.id][question_index[message.chat.id]]
            await bot.send_message(message.chat.id, f'Сколько часов вы были заняты на проекте {project} вчера?')
            questions[message.chat.id] = project
            question_index[message.chat.id] += 1
            await Form.final_answer2.set()
        except IndexError:
            await bot.send_message(message.chat.id, f'Спасибо, ваши данные учтены')
            await state.finish()


async def checking():
    global report, lost_names
    check_answers()
    for key, value in report.items():
        if isinstance(value, float):
            report[key] = round(float(value), 2)
        else:
            report[key] = value
    await bot.send_message(manager_group_id, f'{report}')
    if lost_names:
        await bot.send_message(manager_group_id, f'{lost_names} не указали ФИО боту')
    report, lost_names = {}, []


def heatmap(data, row_labels, col_labels, ax=None, cbar_kw={}, cbarlabel="", **kwargs):
    if not ax:
        ax = plt.gca()
    im = ax.imshow(data, **kwargs)
    cbar = ax.figure.colorbar(im, ax=ax, **cbar_kw)
    cbar.ax.set_ylabel(cbarlabel, rotation=-90, va="bottom")
    ax.set_xticks(np.arange(data.shape[1]), labels=col_labels)
    ax.set_yticks(np.arange(data.shape[0]), labels=row_labels)
    ax.tick_params(top=True, bottom=False, labeltop=True, labelbottom=False)
    plt.setp(ax.get_xticklabels(), rotation=-30, ha="right", rotation_mode="anchor")
    ax.spines[:].set_visible(False)
    ax.set_xticks(np.arange(data.shape[1]+1)-.5, minor=True)
    ax.set_yticks(np.arange(data.shape[0]+1)-.5, minor=True)
    ax.grid(which="minor", color="w", linestyle='-', linewidth=3)
    ax.tick_params(which="minor", bottom=False, left=False)
    return im, cbar


def annotate_heatmap(im, data=None, valfmt="{x:.2f}", textcolors=("black", "white"), threshold=None, **textkw):
    if not isinstance(data, (list, np.ndarray)):
        data = im.get_array()
    if threshold is not None:
        threshold = im.norm(threshold)
    else:
        threshold = im.norm(data.max())/2.
    kw = dict(horizontalalignment="center", verticalalignment="center")
    kw.update(textkw)
    if isinstance(valfmt, str):
        valfmt = matplotlib.ticker.StrMethodFormatter(valfmt)
    texts = []
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            kw.update(color=textcolors[int(im.norm(data[i, j]) > threshold)])
            text = im.axes.text(j, i, valfmt(data[i, j], None), **kw)
            texts.append(text)
    return texts


def stat_by_period(period, name):
    i = 0
    pdfs_stat = []
    for dataset in get_statistics(period):
        i += 1
        projects, workers, info = dataset
        info = np.array([info[x] for x in sorted(info)])
        workers = workers.keys()
        fig, ax = plt.subplots(figsize=(20, 15))
        im, cbar = heatmap(info, sorted(projects), workers, ax=ax, cmap="YlGn", cbarlabel="Часов за день")
        texts = annotate_heatmap(im)
        fig.tight_layout()
        plt.savefig(f'daily_stat_{period[0]}_{period[1]}_{i}.pdf')
        pdfs_stat.append(f'daily_stat_{period[0]}_{period[1]}_{i}.pdf')
    pdf_writer = PdfFileWriter()
    for path in pdfs_stat:
        pdf_reader = PdfFileReader(path)
        for page in range(pdf_reader.getNumPages()):
            # Add each page to the writer object
            pdf_writer.addPage(pdf_reader.getPage(page))
    with open(name, 'wb') as out:
        pdf_writer.write(out)


@dp.message_handler(commands=['daily_stat'])
async def get_daily_stats(message: types.Message):
    if message.from_user.id in chat_admins:
        await bot.send_message(message.chat.id, 'Укажите период в формате: 2022-8-3/2022-9-10 \n'
                                                '(первой идет более ранняя дата)')
        await Form.get_daily_stat.set()
    else:
        await bot.send_message(message.chat.id, 'Нет прав')


@dp.message_handler(state=Form.get_daily_stat)
async def get_daily_stats(message: types.Message, state: FSMContext):
    async with state.proxy() as data:
        data['get_daily_stat'] = message.text
    try:
        stat = data['get_daily_stat'].split('/')
        name = f'daily_stat_{stat[0]}_{stat[1]}.pdf'
        stat_by_period((stat[0], stat[1]), name)
        doc = open(name, 'rb')
        await bot.send_document(message.chat.id, doc)
        await state.finish()
    except PermissionError:
        # Error if your doc is opened on server
        await bot.send_message(message.chat.id, 'Файл статистики открыт на сервере. \n'
                                                'Попросите администратора закрыть файл')
        await state.finish()


# USER-PART


@dp.message_handler(content_types=['new_chat_members'])
async def new_user_joined(message: types.Message):
    get_workers_names()
    global group_id
    if message.chat.id == group_id:
        for new_member in message.new_chat_members:
            if new_member.id not in workers_in_db:
                chat_members.append(new_member.id)
                await greetings(new_member.id)


async def greetings(new_member_id):
    global group_id
    await bot.send_message(group_id, f"User_id {new_member_id} \n"
                                     f"Вас добавили в рабочую группу, откройте чат с ботом @FTE_tracker_bot"
                                     f" и нажмите кнопку 'Старт', чтобы он мог получать от вас данные")


@dp.message_handler(commands=['start'])
async def send_full_name(message: types.Message):
    await bot.send_message(message.chat.id, 'Напишите свое ФИО')
    await Form.first_message.set()


@dp.message_handler(state=Form.first_message)
async def first_message_to_bot(message: types.Message, state: FSMContext):
    get_workers_names()
    global workers_in_db
    try:
        if message.chat.id not in workers_in_db:
            data_tuple = (message.chat.id, message.text)
            add_new_member_to_workers_info(data_tuple)
            await bot.send_message(message.chat.id, 'Вы добавлены в базу')
            await state.finish()
        else:
            await state.finish()
    except sqlite3.OperationalError as error_first_message:
        # error_first_message.args ('database is locked',) is a system message when your DB is opened by someone
        if error_first_message.args == ('database is locked',):
            await bot.send_message(message.chat.id, 'База данных используется в данный момент, попросите '
                                                    'администратора выйти из нее и отправьте команду заново')
            await state.finish()


@dp.message_handler(commands=['update'])
async def update(message: types.Message):
    await bot.send_message(message.chat.id, 'Напишите через запятую (пример: Project 1, 10.0):\n'
                                            '1) Название проекта (как в опросе), по которому необходимо '
                                            'изменить количество часов, затраченных на проект\n'
                                            '2) Верное количество часов c точкой в виде разделителя (пример 2.5)')
    await Form.message_update.set()


@dp.message_handler(state=Form.message_update)
async def msg_update(message: types.Message, state: FSMContext):
    async with state.proxy() as data:
        data['message_update'] = message.text
    message_update = data['message_update'].split(',')
    message_update = list(map(lambda x: x.strip(), message_update))
    if len(message_update) != 2:
        await bot.send_message(message.chat.id, 'Вы ввели данные неверно, введите их заново')
        await Form.message_update.set()
    else:
        try:
            projects = get_projects_names(message.chat.id)
            if message_update[0] in projects:
                now = f'{datetime.datetime.now().year}-{datetime.datetime.now().month}-'f'{datetime.datetime.now().day}'
                data_tuple = (message_update[1], message.chat.id, now, message_update[0])
                update_in_fte_info(data_tuple)
                await bot.send_message(message.chat.id, f'Часы работы на проекте {message_update[0]} обновлены')
                await state.finish()
            else:
                await bot.send_message(message.chat.id, 'Вы не выбрали проект с таким именем')
                await Form.message_update.set()
        except sqlite3.OperationalError as error_message_update:
            # error_message_update.args ('database is locked',) is a system message when your DB is opened by someone
            if error_message_update.args == ('database is locked',):
                await bot.send_message(message.chat.id, 'База данных используется в данный момент, попросите '
                                                        'администратора выйти из нее и отправьте команду заново')
                await state.finish()


if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
