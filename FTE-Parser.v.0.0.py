from aiogram import Bot, Dispatcher, executor, types
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher import FSMContext
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils.exceptions import PollHasAlreadyBeenClosed
from aiogram.dispatcher.filters import Text
from pyrogram import Client
import datetime
import aioschedule
import asyncio
import sqlite3
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import itertools
from PyPDF2 import PdfFileReader, PdfFileWriter
import os

# 1. Install libs: pip install aiogram pip install pyrogram pip install aioschedule pip install asyncio
# pip install matplotlib pip install PyPDF2 pip install pandas pip3 install openpyxl pip install PyInstaller
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

bot_token = ''
api_hash, api_id = '', 0
group_id, manager_group_id = 0, 0
chat_admins = [0, 0]

polls, projects_by_polls, polls_num, polls_close = {}, {}, {}, {}
questions, chosen_projects, question_index, report, workers_in_db = {}, {}, {}, {}, {}
projects_set_by_admin, projects_set_by_admin_div, chat_members, lost_names = [], [], [], []

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


# Export documents functions


def get_stat_by_period_excel(period, name):
    with sqlite3.connect('sqlite_python.db') as conn11:
        cursor11 = conn11.cursor()
        select_info = f"SELECT full_name, project, hours FROM FTE_info INNER JOIN workers_info WHERE " \
                      f"FTE_info.user_id = workers_info.user_id AND " \
                      f"date BETWEEN '{period[0]}' AND '{period[1]}' ORDER BY " \
                      f"full_name"
        cursor11.execute(select_info)
        names = cursor11.fetchall()
    workers, projects = {}, {}
    df_workers, df_projects = [], []
    for row in names:
        if row[0] not in workers:
            workers[row[0]] = {row[1]: row[2]}
        else:
            if row[1] not in workers[row[0]]:
                workers[row[0]][row[1]] = row[2]
            else:
                workers[row[0]][row[1]] += row[2]
        if row[1] not in projects:
            projects[row[1]] = {row[0]: row[2]}
        else:
            if row[0] not in projects[row[1]]:
                projects[row[1]][row[0]] = row[2]
            else:
                projects[row[1]][row[0]] += row[2]

    for worker in workers:
        sum_hours = 0
        for project in workers[worker]:
            sum_hours += workers[worker][project]
        for project in workers[worker]:
            df_workers.append([worker, project, workers[worker][project],
                               round(workers[worker][project] / sum_hours, 2)])
        df_workers.append([worker, ' Итого', round(sum_hours, 2), round(sum_hours / sum_hours, 2)])

    for project in projects:
        sum_hours = 0
        for worker in projects[project]:
            sum_hours += projects[project][worker]
        for worker in projects[project]:
            df_projects.append([project, worker, projects[project][worker],
                                round(projects[project][worker] / sum_hours, 2)])
        df_projects.append([project, ' Итого', round(sum_hours, 2), round(sum_hours / sum_hours, 2)])

    df1 = pd.DataFrame(df_workers, index=[i for i in range(len(df_workers))], columns=['ФИО', 'Проект', 'Часы', "%"])
    df2 = pd.DataFrame(df_projects, index=[i for i in range(len(df_projects))], columns=['Проект', 'ФИО', 'Часы', '%'])
    with pd.ExcelWriter(name) as writer:
        df1.to_excel(writer, sheet_name='По работникам')
        df2.to_excel(writer, sheet_name='По проектам')


def get_statistics_to_pdf(period):
    with sqlite3.connect('sqlite_python.db') as conn10:
        cursor10 = conn10.cursor()
        select_info = f"SELECT full_name, project, hours FROM FTE_info INNER JOIN workers_info WHERE " \
                      f"FTE_info.user_id = workers_info.user_id AND " \
                      f"date BETWEEN '{period[0]}' AND '{period[1]}' ORDER BY " \
                      f"full_name"
        cursor10.execute(select_info)
        names = cursor10.fetchall()
    workers, projects, workers_all_projects, projects_all_workers = {}, {}, {}, {}
    projects_sums, workers_sums, tem_dict = {}, {}, {}
    df_workers, i = [], 0
    for row in names:
        if row[0] not in workers:
            workers[row[0]] = {row[1]: row[2]}
        else:
            if row[1] not in workers[row[0]]:
                workers[row[0]][row[1]] = row[2]
            else:
                workers[row[0]][row[1]] += row[2]
        if row[1] not in projects:
            projects[row[1]] = {row[0]: row[2]}
        else:
            if row[0] not in projects[row[1]]:
                projects[row[1]][row[0]] = row[2]
            else:
                projects[row[1]][row[0]] += row[2]

    for worker in sorted(workers):
        sum_projects = 0
        for project in workers[worker]:
            sum_projects += workers[worker][project]
        workers_sums[worker] = (round(sum_projects, 2))
        workers_all_projects[worker] = []
        for project in sorted(projects):
            if project in workers[worker]:
                workers_all_projects[worker].append(round(workers[worker][project], 2))
            else:
                workers_all_projects[worker].append(0)
        workers_all_projects[worker].append(round(sum_projects, 2))

    for project in sorted(projects):
        sum_projects = 0
        for worker in projects[project]:
            sum_projects += projects[project][worker]
        projects_sums[project] = (round(sum_projects, 2))
        projects_all_workers[project] = []
        for worker in sorted(workers):
            if worker in projects[project]:
                projects_all_workers[project].append(round(projects[project][worker], 2))
            else:
                projects_all_workers[project].append(0)
        projects_all_workers[project].append(round(sum_projects, 2))

    projects_sums['Итого'] = 0

    for worker in workers_all_projects:
        i += 1
        if i != len(workers_all_projects):
            if i % 20 != 0:
                tem_dict[worker] = workers_all_projects[worker]
            else:
                tem_dict[worker] = workers_all_projects[worker]
                tem_dict[' Итого'] = [projects_sums[x] for x in projects_sums]
                df_workers.append(tem_dict)
                tem_dict = {}
        else:
            tem_dict[worker] = workers_all_projects[worker]
            tem_dict[' Итого'] = [projects_sums[x] for x in projects_sums]
            df_workers.append(tem_dict)
            tem_dict = {}

    pdf_columns = itertools.repeat([x for x in projects_sums])
    pdf_rows = [[y for y in x] for x in df_workers]
    pdf_df = df_workers

    projects_sums2 = [' Итого']

    pdf_rows2 = itertools.repeat(projects_sums2 + [x for x in projects_sums if x != 'Итого'])
    pdf_columns2 = [[y for y in x if y != ' Итого'] for x in df_workers]
    pdf_df2 = []
    for workers_group in pdf_columns2:
        temp_dict2 = {}
        for project in projects:
            temp_dict2[project] = []
            for worker in workers_group:
                if worker in projects[project]:
                    temp_dict2[project].append(projects[project][worker])
                else:
                    temp_dict2[project].append(0)
            temp_dict2[' Итого'] = []
            for worker in workers_group:
                temp_dict2[' Итого'].append(workers_sums[worker])
        pdf_df2.append(temp_dict2)

    return zip(pdf_rows, pdf_columns, pdf_df), zip(pdf_rows2, pdf_columns2, pdf_df2)


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
    ax.set_xticks(np.arange(data.shape[1] + 1) - .5, minor=True)
    ax.set_yticks(np.arange(data.shape[0] + 1) - .5, minor=True)
    ax.grid(which="minor", color="w", linestyle='-', linewidth=3)
    ax.tick_params(which="minor", bottom=False, left=False)
    return im, cbar


def annotate_heatmap(im, data=None, valfmt="{x:.2f}", textcolors=("black", "white"), threshold=None, **textkw):
    if not isinstance(data, (list, np.ndarray)):
        data = im.get_array()
    if threshold is not None:
        threshold = im.norm(threshold)
    else:
        threshold = im.norm(data.max()) / 2.
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


def get_stat_by_period_pdf(period, name):
    i = 0
    pdfs_stat = []
    for zips in get_statistics_to_pdf(period):
        for dataset in zips:
            i += 1
            pdf_rows, pdf_columns, pdf_df = dataset
            pdf_df = np.array([pdf_df[x] for x in sorted(pdf_df)])
            fig, ax = plt.subplots(figsize=(20, 15))
            im, cbar = heatmap(pdf_df, sorted(pdf_rows), pdf_columns, ax=ax, cmap="YlGn", cbarlabel="Часов за день")
            texts = annotate_heatmap(im)
            fig.tight_layout()
            plt.savefig(f'period_stat_{period[0]}_{period[1]}_{i}.pdf')
            pdfs_stat.append(f'period_stat_{period[0]}_{period[1]}_{i}.pdf')
    pdf_writer = PdfFileWriter()
    for path in pdfs_stat:
        pdf_reader = PdfFileReader(path)
        for page in range(pdf_reader.getNumPages()):
            pdf_writer.addPage(pdf_reader.getPage(page))
    with open(name, 'wb') as out:
        pdf_writer.write(out)


def get_statistics_to_pie_projects(period):
    with sqlite3.connect('sqlite_python.db') as conn13:
        cursor13 = conn13.cursor()
        select_info = f"SELECT full_name, project, hours FROM FTE_info INNER JOIN workers_info WHERE " \
                      f"FTE_info.user_id = workers_info.user_id AND " \
                      f"date BETWEEN '{period[0]}' AND '{period[1]}' ORDER BY " \
                      f"full_name"
        cursor13.execute(select_info)
        names = cursor13.fetchall()
    to_df, workers_num = {}, {}
    df, df1 = [], []
    for row in names:
        if row[1] not in to_df:
            to_df[row[1]] = {row[0]: row[2]}
        else:
            if row[0] not in to_df[row[1]]:
                to_df[row[1]][row[0]] = row[2]
            else:
                to_df[row[1]][row[0]] += row[2]
    for project in to_df:
        sum_projects = 0
        workers = 0
        for worker in to_df[project]:
            df.append([project, worker, to_df[project][worker]])
            sum_projects += to_df[project][worker]
            workers += 1
        df1.append([round(sum_projects, 2), project])
        workers_num[round(sum_projects, 2)] = workers
    return sorted(df1, reverse=True), workers_num


def pie(period, name):
    data = get_statistics_to_pie_projects((period[0], period[1]))
    labels = [x[1] for x in data[0]]
    sizes = [x[0] for x in data[0]]
    workers = data[1]
    explode = [0.085] + [0.015] * (len(labels) - 1)
    colors = plt.get_cmap('YlGn')(np.linspace(0.8, 0.2, len(labels)))
    fig1, ax1 = plt.subplots(figsize=(12, 8))
    ax1.pie(sizes, explode=explode, labels=labels, autopct=lambda p: '{:.1f}%\n({:,.0f})\n{:,.0f}'
            .format(p, p * sum(sizes) / 100, workers[round(p * sum(sizes) / 100, 2)]),
            shadow=False, startangle=90, colors=colors)
    plt.subplots_adjust(top=0.993, bottom=0.033, left=0, right=1, hspace=0, wspace=0)
    ax1.legend(['X% - от общего времени на проекты', '(X) - Натуральное выражение (часы)',
                'X - Количество сотрудников на проекте'], loc='upper left')
    ax1.axis('equal')
    plt.savefig(name)


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
    get_period_stat = State()


@dp.message_handler(state='*', commands=['cancel'])
@dp.message_handler(Text(equals='cancel', ignore_case=True), state='*')
async def cancel_handler(message: types.Message, state: FSMContext):
    # Allow user to cancel any action
    current_state = await state.get_state()
    if current_state is None:
        return
    await message.reply('Команда отменена.', reply_markup=types.ReplyKeyboardRemove())
    await state.finish()


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
        temp.append('Не участвовал в вышеперечисленных проектах')
        projects_set_by_admin_div.append(temp)
    await state.finish()


@dp.message_handler(commands=['send_polls'])
async def send_polls(message: types.Message):
    # Attribute of function cant be removed cause without it function raises error
    if message.from_user.id in chat_admins:
        aioschedule.every().day.at('08:00').do(create_polls)
        aioschedule.every().day.at('09:00').do(checking)
        aioschedule.every().day.at('10:00').do(monthly_checking)
        aioschedule.every().day.at('08:00').do(clean_folder)
        while True:
            await aioschedule.run_pending()
            await asyncio.sleep(1)
    else:
        await bot.send_message(message.chat.id, 'Нет прав')


async def create_polls():
    global projects_set_by_admin_div, chat_members, polls, polls_num, projects_by_polls, question_index, polls_close
    polls, polls_num, projects_by_polls, question_index, polls_close = {}, {}, {}, {}, {}
    for user in chat_members:
        i = -1
        for group_projects in projects_set_by_admin_div:
            new_poll = await bot.send_poll(user, 'В каких проектах вы принимали участие вчера?',
                                           group_projects, is_anonymous=False, allows_multiple_answers=True)
            polls[new_poll.poll.id] = group_projects
            if new_poll.chat.id not in polls_close:
                polls_close[new_poll.chat.id] = {new_poll.poll.id: [new_poll.message_id, i + 1]}
                i += 1
            else:
                polls_close[new_poll.chat.id][new_poll.poll.id] = [new_poll.message_id, i + 1]
                i += 1


@dp.poll_answer_handler()
async def voting(call):
    global chosen_projects, worker_daily_stat, projects_by_polls, polls_num, projects_set_by_admin_div, polls_close
    if not call.option_ids:
        polls_num[call.user.id] += 1
        await bot.stop_poll(call.user.id, polls_close[call.user.id][call.poll_id][0])
        del projects_by_polls[call.user.id][call.poll_id]
        await bot.send_message(call.user.id, "Вы отозвали голос из одного из опросов.\n"
                                             "Вам необходимо проголосовать заново")
        new_poll = await bot.send_poll(call.user.id, 'В каких проектах вы принимали участие вчера?',
                                       projects_set_by_admin_div[polls_close[call.user.id][call.poll_id][1]],
                                       is_anonymous=False, allows_multiple_answers=True)
        polls[new_poll.poll.id] = projects_set_by_admin_div[polls_close[call.user.id][call.poll_id][1]]
        polls_close[new_poll.chat.id][new_poll.poll.id] = \
            [new_poll.message_id, polls_close[call.user.id][call.poll_id][1]]
    else:
        if call.user.id not in polls_num:
            polls_num[call.user.id] = len(projects_set_by_admin_div)
        polls_num[call.user.id] -= 1
        for project_num in call.option_ids:
            if call.user.id not in projects_by_polls:
                projects_by_polls[call.user.id] = {call.poll_id: [polls[call.poll_id][project_num]]}
            else:
                if call.poll_id not in projects_by_polls[call.user.id]:
                    projects_by_polls[call.user.id][call.poll_id] = [polls[call.poll_id][project_num]]
                else:
                    projects_by_polls[call.user.id][call.poll_id].append(polls[call.poll_id][project_num])
        if polls_num[call.user.id] == 0:
            chosen_projects[call.user.id] = [pr for poll in projects_by_polls[call.user.id]
                                             for pr in projects_by_polls[call.user.id][poll]
                                             if pr != 'Не участвовал в вышеперечисленных проектах']
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, selective=True)
            markup.add("Продолжить")
            await bot.send_message(call.user.id, "Нажмите продолжить", reply_markup=markup)
            await Form.final_answer.set()


@dp.message_handler(lambda message: message.text in ["Продолжить"], state=Form.final_answer)
async def create_questions_from_polls(message: types.Message, state: FSMContext):
    # Attribute of function cant be removed cause without it function raises error
    global question_index, chosen_projects, polls_close
    markup_remove = types.ReplyKeyboardRemove()
    for poll_to_close in polls_close[message.chat.id]:
        try:
            await bot.stop_poll(message.chat.id, polls_close[message.chat.id][poll_to_close][0])
        except PollHasAlreadyBeenClosed:
            pass
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


@dp.message_handler(commands=['period_stat'])
async def get_period_stats(message: types.Message):
    if message.from_user.id in chat_admins:
        await bot.send_message(message.chat.id, 'Укажите период в формате: 2022-8-3/2022-9-10 \n'
                                                '(первой идет более ранняя дата).\n'
                                                'Если нужна статистика за 1 день - 2022-8-3/2022-8-3')
        await Form.get_period_stat.set()
    else:
        await bot.send_message(message.chat.id, 'Нет прав')


@dp.message_handler(state=Form.get_period_stat)
async def get_period_stats(message: types.Message, state: FSMContext):
    async with state.proxy() as data:
        data['get_period_stat'] = message.text
    try:
        stat = data['get_period_stat'].split('/')
        name_excel = f'period_stat_{stat[0]}_{stat[1]}.xlsx'
        get_stat_by_period_excel((stat[0], stat[1]), name_excel)
        doc1 = open(name_excel, 'rb')
        name_pdf = f'period_stat_{stat[0]}_{stat[1]}_workers.pdf'
        get_stat_by_period_pdf((stat[0], stat[1]), name_pdf)
        doc2 = open(name_pdf, 'rb')
        name_pie_projects = f'period_stat_{stat[0]}_{stat[1]}_projects_pie.png'
        pie((stat[0], stat[1]), name_pie_projects)
        doc3 = open(name_pie_projects, 'rb')
        await bot.send_message(message.chat.id, 'Excel-файл содержит два листа. Информация упорядочена в первом '
                                                'по работникам, во втором - по проектам.')
        await bot.send_document(message.chat.id, doc1)
        await bot.send_document(message.chat.id, doc2)
        await bot.send_document(message.chat.id, doc3)
        await state.finish()
    except PermissionError:
        # Error if your doc is opened on server
        await bot.send_message(message.chat.id, 'Файл статистики открыт на сервере. \n'
                                                'Попросите администратора закрыть файл')
        await state.finish()
    except (IndexError, ValueError):
        # Error if user input wrong data
        await bot.send_message(message.chat.id, 'Вы ввели данные неверно.\nПопробуйте снова')
        await Form.get_period_stat.set()


async def monthly_checking():
    y, m, d = datetime.datetime.now().year, datetime.datetime.now().month, datetime.datetime.now().day
    now = datetime.date(y, m, d)
    if d == 1:
        if m == 1:
            first_d_of_previous_m = datetime.date(y - 1, 12, 1)
        else:
            first_d_of_previous_m = datetime.date(y, m - 1, 1)
        delta = abs((now - first_d_of_previous_m).days)
        first_d_of_previous_m = f'{first_d_of_previous_m.year}-{first_d_of_previous_m.month}-' \
                                f'{first_d_of_previous_m.day}'
        last_d_of_previous_m = datetime.date(y, m - 1, delta)
        last_d_of_previous_m = f'{last_d_of_previous_m.year}-{last_d_of_previous_m.month}-{last_d_of_previous_m.day}'
        try:
            name_excel = f'monthly_stat_{first_d_of_previous_m}_{last_d_of_previous_m}.xlsx'
            get_stat_by_period_excel((first_d_of_previous_m, last_d_of_previous_m), name_excel)
            doc1 = open(name_excel, 'rb')
            name_pdf = f'monthly_stat_{first_d_of_previous_m}_{last_d_of_previous_m}_workers.pdf'
            get_stat_by_period_pdf((first_d_of_previous_m, last_d_of_previous_m), name_pdf)
            doc2 = open(name_pdf, 'rb')
            name_pie_projects = f'period_stat_{first_d_of_previous_m}_{last_d_of_previous_m}_projects_pie.png'
            pie((first_d_of_previous_m, last_d_of_previous_m), name_pie_projects)
            doc3 = open(name_pie_projects, 'rb')
            await bot.send_message(manager_group_id, 'Excel-файл содержит два листа. Информация упорядочена в первом '
                                                     'по работникам, во втором - по проектам.')
            await bot.send_document(manager_group_id, doc1)
            await bot.send_document(manager_group_id, doc2)
            await bot.send_document(manager_group_id, doc3)
        except PermissionError:
            # Error if your doc is opened on server
            await bot.send_message(manager_group_id, 'Файл статистики открыт на сервере. \n'
                                                     'Попросите администратора закрыть файлы и выслать вам их вручную ')


async def clean_folder():
    files = [x for x in os.listdir(os.path.dirname(__file__)) if '.pdf' in x or '.png' in x or 'xlsx' in x]
    for file in files:
        path = os.path.join(os.path.abspath(os.path.dirname(__file__)), file)
        os.remove(path)


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
