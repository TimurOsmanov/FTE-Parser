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
import os

# 1. Install libs: pip install aiogram pip install pyrogram pip install aioschedule pip install asyncio
# pip install pandas pip install PyInstaller (for exe)
# You can install SQLite to monitor changes in DB. To install it for Windows download 2 files
# from https://www.sqlite.org/download.html from Precompiled Binaries for Windows:
# first is sqlite-dll-win32-x86-3390200.zip or sqlite-dll-win64-x64-3390200.zip according to your system (32/64),
# second sqlite-tools-win32-x86-3390200.zip
# 2. Firstly you have to login in https://my.telegram.org/ to manage your apps using Telegram API.
# On the first run you'll be asked to enter your phone number or
# bot token (i use my phone) to login https://my.telegram.org/.
# You have to enter confirmation code sent to your phone.
# Than for using pyrogram you have to enter bot token (5561259498:AAFhMQx3LJ_EAiqkz4IThCrecmgMpr7IdC8)
# Now you can use app
# 3. Bot will work correctly if you create group and then add bot there

bot_token = ''
api_hash, api_id = '', 0
group_id, manager_group_id = 0, 0
chat_admins, chat_managers = [], []

polls, projects_by_polls, polls_num, polls_close = {}, {}, {}, {}
questions, chosen_projects, question_index, report, workers_in_db = {}, {}, {}, {}, {}
projects_set_by_admin, projects_set_by_admin_div, chat_members, lost_names = [], [], [], []
reminder_index = 0

bot = Bot(bot_token)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)


# Parsing of chat members, chat members will receive polls with projects, chat managers can get statistics and update
# data in database


def get_chat_members():
    with Client("my_account", api_id, api_hash) as app:
        for member in app.get_chat_members(group_id):
            if not member.user.is_bot:
                chat_members.append(member.user.id)


get_chat_members()


def get_chat_managers():
    with Client("my_account", api_id, api_hash) as app:
        for member in app.get_chat_members(manager_group_id):
            if not member.user.is_bot:
                chat_managers.append(member.user.id)


get_chat_managers()


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
    global report, lost_names, workers_in_db
    get_workers_names()
    with sqlite3.connect('sqlite_python.db') as conn6:
        now = datetime.datetime.now() - datetime.timedelta(days=1)
        now = now.strftime("%Y-%m-%d")
        cursor6 = conn6.cursor()
        select_info = f"SELECT user_id, SUM(hours) FROM FTE_info WHERE date = '{now}' GROUP BY user_id"
        cursor6.execute(select_info)
        daily_stat_raw = cursor6.fetchall()
    daily_stat = {user[0]: user[1] for user in daily_stat_raw}
    for worker in chat_members:
        try:
            if worker not in report:
                if worker in daily_stat:
                    if not 6 <= daily_stat[worker] <= 12:
                        report[workers_in_db[worker]] = daily_stat[worker]
                else:
                    if workers_in_db[worker] not in report:
                        report[workers_in_db[worker]] = 'Не ответил/не работал'
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


def get_data_for_excel(col1, col2, period):
    with sqlite3.connect('sqlite_python.db') as conn10:
        cursor10 = conn10.cursor()
        insert_info = f"""
        with final_table as (select distinct {col1}, {col2}, sum(hours) over (partition by {col1}, {col2}) as hours,
        (sum(hours) over (partition by {col1}, {col2}) / sum(hours) over (partition by {col1})) as percent from fte_info 
        inner join workers_info using(user_id)
        where date between '{period[0]}' and '{period[1]}'), 
        final as (select {col1}, {col2}, hours, percent * 100 as percent from final_table
        union all
        select {col1}, '   Итого', sum(hours), sum(percent) * 100 from final_table
        group by 1, 2
        union all
        select '   Итого', '   Итого', sum(hours), null from final_table
        ), 
        
        days_table as (select distinct {col1}, {col2}, count(date) over (partition by {col1}, {col2}) as days from 
        (select distinct {col1}, {col2}, date from fte_info
        inner join workers_info on fte_info.user_id = workers_info.user_id
        where date between '{period[0]}' and '{period[1]}')
        union all 
        select distinct {col1}, '   Итого', count(date) over (partition by {col1}) as days from 
        (select distinct {col1}, '   Итого', date from fte_info
        inner join workers_info on fte_info.user_id = workers_info.user_id
        where date between '{period[0]}' and '{period[1]}')
        union all 
        select '   Итого', '   Итого', count(date) from (select distinct null, null, date from FTE_info
        where date between '{period[0]}' and '{period[1]}'))
        
        select {col1}, {col2}, round(hours, 2), round(percent, 2), days from final 
        left join days_table using({col1}, {col2})
        order by 1, 2 """
        cursor10.execute(insert_info)
        data = cursor10.fetchall()
    return data


def get_info_for_update():
    with sqlite3.connect('sqlite_python.db') as conn11:
        cursor11 = conn11.cursor()
        info = """select distinct user_id, full_name from fte_info inner join workers_info using(user_id)"""
        cursor11.execute(info)
        users = cursor11.fetchall()
        info2 = """select distinct project from fte_info"""
        cursor11.execute(info2)
        projects = cursor11.fetchall()
    return users, projects


# Export documents functions


def get_stat_by_period_excel(period, name):
    projects = get_data_for_excel('project', 'full_name', period)
    workers = get_data_for_excel('full_name', 'project', period)

    df_work, df_proj = [], []
    for project in projects:
        df_proj.append(project)
    for worker in workers:
        df_work.append(worker)

    df1 = pd.DataFrame(df_work, index=list(range(1, len(df_work) + 1)), columns=['ФИО', 'Проект', 'Часы', "%", 'Дни'])
    df2 = pd.DataFrame(df_proj, index=list(range(1, len(df_proj) + 1)), columns=['Проект', 'ФИО', 'Часы', '%', 'Дни'])
    with pd.ExcelWriter(name) as writer:
        df1.to_excel(writer, sheet_name='По работникам')
        df2.to_excel(writer, sheet_name='По проектам')


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
    update_by_manager = State()


async def create_polls():
    global projects_set_by_admin_div, chat_members, polls, polls_num, projects_by_polls, question_index, polls_close, \
        reminder_index, report, lost_names, chosen_projects, questions
    polls, polls_num, projects_by_polls, question_index, polls_close = {}, {}, {}, {}, {}
    report, chosen_projects, questions = {}, {}, {}
    lost_names = []
    reminder_index = 0
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
        await bot.send_message(user, 'Если у вас выходной, для завершения опроса используйте команду /close_polls.')


async def clean_folder():
    files = [x for x in os.listdir(os.path.dirname(__file__)) if '.pdf' in x or '.png' in x or 'xlsx' in x]
    for file in files:
        path = os.path.join(os.path.abspath(os.path.dirname(__file__)), file)
        os.remove(path)


async def monthly_checking():
    y, m, d = datetime.datetime.now().year, datetime.datetime.now().month, datetime.datetime.now().day
    # d = 1 for test
    now = datetime.date(y, m, d)
    if d == 1:
        last_d_of_previous_m = now - datetime.timedelta(days=1)
        y1, m1 = last_d_of_previous_m.year, last_d_of_previous_m.month
        last_d_of_previous_m = last_d_of_previous_m.strftime("%Y-%m-%d")
        first_d_of_previous_m = datetime.datetime(y1, m1, 1).strftime("%Y-%m-%d")
        try:
            name_excel = f'monthly_stat_{first_d_of_previous_m}_{last_d_of_previous_m}.xlsx'
            get_stat_by_period_excel((first_d_of_previous_m, last_d_of_previous_m), name_excel)
            doc1 = open(name_excel, 'rb')
            await bot.send_message(manager_group_id, 'Excel-файл содержит два листа. Информация упорядочена в первом '
                                                     'по работникам, во втором - по проектам.')
            await bot.send_document(manager_group_id, doc1)
            await clean_folder()
        except PermissionError:
            # Error if your doc is opened on server
            await bot.send_message(manager_group_id, 'Файл статистики открыт на сервере. \n'
                                                     'Попросите администратора закрыть файлы и выслать вам их вручную.')


async def checking():
    global report, lost_names, reminder_index
    reminder_index += 1
    check_answers()
    if reminder_index == 1:
        for key, value in report.items():
            if isinstance(value, float):
                report[key] = round(float(value), 2)
            else:
                report[key] = value
        if report:
            checking_report = 'Сведения о сотрудниках, которыe работали меньше 6 часов или больше 12:\n\n'
            for num, worker_status in enumerate(report.items()):
                worker, status = worker_status
                worker = worker.replace("'", "")
                if isinstance(status, str):
                    status = status.replace("'", "")
                checking_report += str(num + 1) + '. ' + worker + ': ' + str(status) + '\n'
            await bot.send_message(manager_group_id, checking_report)
        if lost_names:
            await bot.send_message(manager_group_id, f'{lost_names} не указали ФИО боту.')
    else:
        workers_in_db_rev = {value: key for key, value in workers_in_db.items()}
        if any([worker_hours == 'Не ответил/не работал' for worker_hours in report.values()]):
            for worker in [worker for worker in report if report[worker] == 'Не ответил/не работал']:
                await bot.send_message(workers_in_db_rev[worker], 'Вы забыли ответить на опросы, '
                                                                  'вам необходимо на них ответить.')


async def close_all_polls():
    global polls_close
    for worker in polls_close:
        for poll in polls_close[worker]:
            await bot.stop_poll(worker, polls_close[worker][poll][0])


# Cancel command is available for all types of users at any step of conversation
@dp.message_handler(state='*', commands=['cancel'])
@dp.message_handler(Text(equals='cancel', ignore_case=True), state='*')
async def cancel_handler(message: types.Message, state: FSMContext):
    # Allow user to cancel any action
    current_state = await state.get_state()
    if current_state is None:
        return
    await message.reply('Команда отменена.', reply_markup=types.ReplyKeyboardRemove())
    await state.finish()


# Cancel command is available for all types of users at any step of conversation
@dp.message_handler(state='*', commands=['close_polls'])
@dp.message_handler(Text(equals='close_polls', ignore_case=True), state='*')
async def close_polls(message: types.Message, state: FSMContext):
    global polls_close, report
    if polls_close:
        for poll in polls_close[message.chat.id]:
            await bot.stop_poll(message.chat.id, polls_close[message.chat.id][poll][0])
    await message.reply('Опрос закрыт. Приятного отдыха.', reply_markup=types.ReplyKeyboardRemove())
    report[workers_in_db[message.chat.id]] = 'Выходной'
    await state.finish()


@dp.message_handler(commands=['set_projects'])
async def set_projects(message: types.Message):
    global chat_admins
    if message.from_user.id in chat_admins:
        await message.reply("Перечислите проекты через запятую.")
        await Form.project.set()
    else:
        await bot.send_message(message.chat.id, 'Нет прав.')


@dp.message_handler(state=Form.project)
async def admin_set_projects(message: types.Message, state: FSMContext):
    global projects_set_by_admin, projects_set_by_admin_div
    projects_set_by_admin, projects_set_by_admin_div = [], []
    async with state.proxy() as data:
        data['project_name'] = message.text
    projects_set_by_admin = data['project_name'].split(',')
    projects_set_by_admin = list(map(lambda x: x.strip(), projects_set_by_admin))
    if all(projects_set_by_admin):
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
    else:
        await bot.send_message(message.chat.id, 'Вы ввели некорректную информацию, попробуйте заново.')


@dp.message_handler(commands=['send_polls'])
async def send_polls(message: types.Message):
    global chat_admins, reminder_index
    await create_polls()
    if message.from_user.id in chat_admins:
        aioschedule.every().day.at('09:00').do(create_polls)
        aioschedule.every().day.at('09:30').do(monthly_checking)
        aioschedule.every().day.at('10:00').do(checking)
        aioschedule.every().day.at('10:30').do(checking)
        aioschedule.every().day.at('11:00').do(checking)
        aioschedule.every().day.at('11:30').do(checking)
        aioschedule.every().day.at('12:00').do(checking)
        aioschedule.every().day.at('12:30').do(checking)
        aioschedule.every().day.at('13:00').do(checking)
        aioschedule.every().day.at('13:30').do(checking)
        aioschedule.every().day.at('14:00').do(checking)
        aioschedule.every().day.at('14:30').do(checking)
        aioschedule.every().day.at('15:00').do(checking)
        aioschedule.every().day.at('15:30').do(checking)
        aioschedule.every().day.at('16:00').do(checking)
        aioschedule.every().day.at('16:30').do(checking)
        aioschedule.every().day.at('17:00').do(checking)
        aioschedule.every().day.at('17:30').do(checking)
        aioschedule.every().day.at('18:00').do(checking)
        aioschedule.every().day.at('18:30').do(checking)
        aioschedule.every().day.at('19:00').do(checking)
        aioschedule.every().day.at('19:30').do(close_all_polls)
        while True:
            await aioschedule.run_pending()
            await asyncio.sleep(1)
    else:
        await bot.send_message(message.chat.id, 'Нет прав.')


@dp.poll_answer_handler()
async def voting(call):
    global chosen_projects, worker_daily_stat, projects_by_polls, polls_num, projects_set_by_admin_div, polls_close
    if not call.option_ids:
        polls_num[call.user.id] += 1
        await bot.stop_poll(call.user.id, polls_close[call.user.id][call.poll_id][0])
        del projects_by_polls[call.user.id][call.poll_id]
        await bot.send_message(call.user.id, "Вы отозвали голос из одного из опросов. "
                                             "Вам необходимо проголосовать заново.")
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
        await bot.send_message(message.chat.id, f'Либо вы сегодня не работали,\n'
                                                f'либо произошла ошибка, обратитесь к администратору.',
                               reply_markup=markup_remove)
        await state.finish()


@dp.message_handler(lambda message: message.text not in ["Продолжить"], state=Form.final_answer)
async def create_questions_from_polls(message: types.Message):
    await bot.send_message(message.chat.id, f"Вам нужно нажать кнопку 'Продолжить' или написать"
                                            f" Продолжить. ")
    await Form.final_answer.set()


@dp.message_handler(state=Form.final_answer2)
async def create_questions_from_polls(message: types.Message, state: FSMContext):
    global question_index, worker_daily_stat, chosen_projects
    replaced_message_text = message.text.replace(',', '').replace('.', '')
    if not replaced_message_text.isdigit():
        await bot.send_message(message.chat.id, f'Ответ должен быть числом, напишите количество часов заново.')
        await Form.final_answer2.set()
    else:
        async with state.proxy() as data:
            data['answer'] = message.text
        hours = data['answer']
        worker_daily_stat += 1
        now = datetime.datetime.now() - datetime.timedelta(days=1)
        now = now.strftime("%Y-%m-%d")
        data_tuple = (worker_daily_stat, now, questions[message.chat.id], message.chat.id, hours.replace(',', '.'))
        if float(hours.replace(',', '.')) >= 14:
            await bot.send_message(message.chat.id, f'Недопустимое количество часов, напишите количество часов заново.')
            await Form.final_answer2.set()
        else:
            try:
                insert_into_db(data_tuple)
            except sqlite3.OperationalError as error_final_answer2:
                # error_final_answer2.args ('database is locked',) is a system message when your DB is opened by someone
                if error_final_answer2.args == ('database is locked',):
                    question_index[message.chat.id] -= 1
                    await bot.send_message(message.chat.id, 'База данных используется в данный момент, попросите '
                                                            'администратора закрыть ее и отправьте ответ заново.')
                    await Form.final_answer2.set()
            try:
                project = chosen_projects[message.chat.id][question_index[message.chat.id]]
                await bot.send_message(message.chat.id, f'Сколько часов вы были заняты на проекте {project} вчера?')
                questions[message.chat.id] = project
                question_index[message.chat.id] += 1
                await Form.final_answer2.set()
            except IndexError:
                await bot.send_message(message.chat.id, f'Спасибо, ваши данные учтены.')
                await state.finish()


# MANAGER-PART


async def greetings_manager(new_member_username):
    global manager_group_id
    await bot.send_message(manager_group_id, f"@{new_member_username} \n"
                                             f"Вас добавили в группу менеджеров, откройте чат с ботом @FTE_tracker_bot"
                                             f" и нажмите кнопку 'Старт', чтобы он мог получать от вас данные.")


@dp.message_handler(commands=['period_stat'])
async def get_period_stats(message: types.Message):
    global chat_admins, chat_managers
    if message.from_user.id in chat_admins or message.from_user.id in chat_managers:
        await bot.send_message(message.chat.id, 'Укажите период в формате: 2022-08-03/2022-09-10 \n'
                                                '(первой идет более ранняя дата, формат даты ГГГГ-ММ-ДД).\n'
                                                'Если нужна статистика за 1 день - 2022-08-03/2022-08-03.')
        await Form.get_period_stat.set()
    else:
        await bot.send_message(message.chat.id, 'Нет прав.')


@dp.message_handler(state=Form.get_period_stat)
async def get_period_stats(message: types.Message, state: FSMContext):
    async with state.proxy() as data:
        data['get_period_stat'] = message.text
    try:
        stat = data['get_period_stat'].split('/')
        name_excel = f'period_stat_{stat[0]}_{stat[1]}.xlsx'
        get_stat_by_period_excel((stat[0], stat[1]), name_excel)
        doc1 = open(name_excel, 'rb')

        await bot.send_message(message.chat.id, 'Excel-файл содержит два листа. Информация упорядочена в первом '
                                                'по работникам, во втором - по проектам.')
        await bot.send_document(message.chat.id, doc1)
        await state.finish()
        await clean_folder()
    except PermissionError:
        # Error if your doc is opened on server
        await bot.send_message(message.chat.id, 'Файл статистики открыт на сервере. \n'
                                                'Попросите администратора закрыть файл и введите команду заново.')
        await state.finish()
    except (IndexError, ValueError):
        # Error if user input wrong data
        await bot.send_message(message.chat.id, 'Либо вы ввели данные неверно, либо за указанный период данных нет.'
                                                '\nПопробуйте снова.')
        await Form.get_period_stat.set()


@dp.message_handler(commands=['update_by_manager'])
async def update_by_manager(message: types.Message):
    global chat_admins, chat_managers
    if message.from_user.id in chat_admins or message.from_user.id in chat_managers:
        await bot.send_message(message.chat.id, 'Для внесения пропущенной записи в базу укажите через запятую:\n'
                                                '1. Дата в формате 2022-08-03 (ГГГГ-ММ-ДД),\n'
                                                '2. Название проекта\n'
                                                '3. ID сотрудника (в следующих сообщениях - список имен в формате '
                                                '(ID сотрудника, имя сотрудника), проектов, указывать без кавычек)\n'
                                                '4. Количество часов в формате 4.5 (4 часа 30 минут).')
        users, projects = get_info_for_update()
        await bot.send_message(message.chat.id, users)
        await bot.send_message(message.chat.id, projects)
        await Form.update_by_manager.set()
    else:
        await bot.send_message(message.chat.id, 'Нет прав.')


@dp.message_handler(state=Form.update_by_manager)
async def update_by_manager(message: types.Message, state: FSMContext):
    async with state.proxy() as data:
        data['get_period_stat'] = message.text
    try:
        date, project, user_id, hours = [x.strip() for x in data['get_period_stat'].split(',')]
        unique_id = get_worker_daily_stat() + 1
        hours = hours.replace(',', '').replace('.', '')
        if not hours.isdigit():
            await bot.send_message(message.chat.id, f'Ответ должен быть числом, напишите количество часов заново.')
            await Form.update_by_manager.set()
        else:
            insert_into_db((unique_id, date, project, user_id, hours))
            await bot.send_message(message.chat.id, 'Запись успешно внесена в базу данных.')
            await state.finish()
    except sqlite3.OperationalError as error_message_update:
        # error_message_update.args ('database is locked',) is a system message when your DB is opened by someone
        if error_message_update.args == ('database is locked',):
            await bot.send_message(message.chat.id, 'База данных используется в данный момент, попросите '
                                                    'администратора выйти из нее и отправьте команду заново.')
            await state.finish()


# USER-PART


async def greetings(new_member_username):
    global group_id
    await bot.send_message(group_id, f"@{new_member_username} \n"
                                     f"Вас добавили в рабочую группу, откройте чат с ботом @FTE_tracker_bot"
                                     f" и нажмите кнопку 'Старт', чтобы он мог получать от вас данные.")


@dp.message_handler(content_types=['new_chat_members'])
async def new_user_joined(message: types.Message):
    get_workers_names()
    global group_id, chat_members, workers_in_db, manager_group_id
    if message.chat.id == group_id:
        for new_member in message.new_chat_members:
            if new_member.id not in workers_in_db:
                chat_members.append(new_member.id)
                await greetings(new_member.username)
            if new_member.id not in chat_members:
                chat_members.append(new_member.id)
                await greetings(new_member.username)

    if message.chat.id == manager_group_id:
        for new_member in message.new_chat_members:
            if new_member.id not in workers_in_db:
                chat_managers.append(new_member.id)
                await greetings_manager(new_member.username)
            if new_member.id not in chat_managers:
                chat_managers.append(new_member.id)
                await greetings_manager(new_member.username)


@dp.message_handler(content_types=['left_chat_member'])
async def user_fired(message: types.Message):
    global chat_members, chat_managers, group_id, manager_group_id
    if message.chat.id == group_id:
        if message.left_chat_member.id in chat_members:
            chat_members.remove(message.left_chat_member.id)

    if message.chat.id == manager_group_id:
        if message.left_chat_member.id in chat_managers:
            chat_managers.remove(message.left_chat_member.id)


@dp.message_handler(commands=['start'])
async def send_full_name(message: types.Message):
    await bot.send_message(message.chat.id, 'Напишите свое ФИО.')
    await Form.first_message.set()


@dp.message_handler(state=Form.first_message)
async def first_message_to_bot(message: types.Message, state: FSMContext):
    get_workers_names()
    global workers_in_db
    try:
        if message.chat.id not in workers_in_db:
            data_tuple = (message.chat.id, message.text)
            add_new_member_to_workers_info(data_tuple)
            await bot.send_message(message.chat.id, 'Вы добавлены в базу.')
            await state.finish()
        else:
            await state.finish()
    except sqlite3.OperationalError as error_first_message:
        # error_first_message.args ('database is locked',) is a system message when your DB is opened by someone
        if error_first_message.args == ('database is locked',):
            await bot.send_message(message.chat.id, 'База данных используется в данный момент, попросите '
                                                    'администратора выйти из нее и отправьте команду заново.')
            await state.finish()


@dp.message_handler(commands=['update'])
async def update(message: types.Message):
    await bot.send_message(message.chat.id, 'Напишите через запятую (пример: Project 1, 10.0):\n'
                                            '1) Название проекта (как в опросе), по которому необходимо '
                                            'изменить количество часов, затраченных на проект\n'
                                            '2) Верное количество часов c точкой в виде разделителя (пример 2.5).')
    await Form.message_update.set()


@dp.message_handler(state=Form.message_update)
async def msg_update(message: types.Message, state: FSMContext):
    async with state.proxy() as data:
        data['message_update'] = message.text
    message_update = data['message_update'].split(',')
    message_update = list(map(lambda x: x.strip(), message_update))
    if len(message_update) != 2:
        await bot.send_message(message.chat.id, 'Вы ввели данные неверно, введите их заново.')
        await Form.message_update.set()
    else:
        try:
            projects = get_projects_names(message.chat.id)
            if message_update[0] in projects:
                now = datetime.datetime.now() - datetime.timedelta(days=1)
                now = now.strftime("%Y-%m-%d")
                data_tuple = (message_update[1], message.chat.id, now, message_update[0])
                update_in_fte_info(data_tuple)
                await bot.send_message(message.chat.id, f'Часы работы на проекте {message_update[0]} обновлены.')
                await state.finish()
            else:
                await bot.send_message(message.chat.id, 'Вы не выбрали проект с таким именем.')
                await Form.message_update.set()
        except sqlite3.OperationalError as error_message_update:
            # error_message_update.args ('database is locked',) is a system message when your DB is opened by someone
            if error_message_update.args == ('database is locked',):
                await bot.send_message(message.chat.id, 'База данных используется в данный момент, попросите '
                                                        'администратора выйти из нее и отправьте команду заново.')
                await state.finish()


if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
