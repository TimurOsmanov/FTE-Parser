from aiogram import Bot, Dispatcher, executor, types
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher import FSMContext
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from pyrogram import Client
import datetime
import aioschedule
import asyncio
import sqlite3
# 1. Install libs: pip install aiogram pip install pyrogram pip install aioschedule pip install asyncio
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
bot_token, api_hash = '', ''
api_id, group_id, manager_group_id = 0, 0, 0
polls, projects_by_polls, questions, chosen_projects, question_index, report, workers_in_db = {}, {}, {}, {}, {}, {}, {}
polls_num = {}
projects_set_by_admin, projects_set_by_admin_div, chat_members, chat_admins = [], [], [], [0, 0]
lost_names = []
bot = Bot(bot_token)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)


def get_chat_members():
    with Client("my_account", api_id, api_hash) as app:
        for member in app.get_chat_members(group_id):
            if not member.user.is_bot:
                chat_members.append(member.user.id)


get_chat_members()


def get_worker_daily_stat():
    # Function gets number of last record to provide it to new insert_into_db
    with sqlite3.connect('sqlite_python.db') as conn1:
        cursor1 = conn1.cursor()
        select_info = 'SELECT unique_id FROM FTE_info'
        cursor1.execute(select_info)
        number = cursor1.fetchall()
        return number[-1][0]


def get_workers_names():
    global workers_in_db
    with sqlite3.connect('sqlite_python.db') as conn2:
        cursor2 = conn2.cursor()
        select_info = 'SELECT user_id, full_name FROM workers_info'
        cursor2.execute(select_info)
        names = cursor2.fetchall()
        workers_in_db = {user[0]: user[1] for user in names}


get_workers_names()


def insert_into_db(data_tuple):
    # Function inserts new record according to new answer in poll
    with sqlite3.connect('sqlite_python.db') as conn3:
        cursor3 = conn3.cursor()
        insert_info = '''INSERT INTO FTE_info (unique_id, date, project, 
        user_id, hours) VALUES (?, ?, ?, ?, ?)'''
        cursor3.execute(insert_info, data_tuple)


def check_answers():
    global report, lost_names
    get_workers_names()
    with sqlite3.connect('sqlite_python.db') as conn4:
        now = (f'{datetime.datetime.now().year}-{datetime.datetime.now().month}-'
               f'{datetime.datetime.now().day}')
        cursor4 = conn4.cursor()
        select_info = f"SELECT user_id, SUM(hours) FROM FTE_info WHERE date = '{now}' GROUP BY user_id"
        cursor4.execute(select_info)
        daily_stat_raw = cursor4.fetchall()
        daily_stat = {user[0]: user[1] for user in daily_stat_raw}
        for worker in chat_members:
            try:
                if worker in daily_stat:
                    if not 6 <= daily_stat[worker] <= 12:
                        report[workers_in_db[worker]] = daily_stat[worker]
                else:
                    report[workers_in_db[worker]] = 'не ответил/не работал'
            except KeyError:
                lost_names.append(worker)


def add_new_member_to_workers_info(data_tuple):
    with sqlite3.connect('sqlite_python.db') as conn5:
        cursor5 = conn5.cursor()
        insert_info = '''INSERT INTO workers_info (user_id, full_name) VALUES (?, ?)'''
        cursor5.execute(insert_info, data_tuple)


try:
    with sqlite3.connect('sqlite_python.db') as conn6:
        cursor6 = conn6.cursor()
        create_table = '''CREATE TABLE FTE_info (
                                unique_id INTEGER PRIMARY KEY,
                                date datetime,
                                project text,
                                user_id INTEGER,
                                hours REAL NOT NULL)'''
        cursor6.execute(create_table)
        worker_daily_stat = 0
except sqlite3.Error as error:
    # error.args ('table FTE_info already exists',) is a system message when your DB already has table
    # IndexError raises when table is empty
    if error.args == ('table FTE_info already exists',):
        try:
            worker_daily_stat = get_worker_daily_stat()
        except IndexError:
            worker_daily_stat = 0
    else:
        print("Ошибка при подключении к sqlite", error)

try:
    with sqlite3.connect('sqlite_python.db') as conn7:
        cursor7 = conn7.cursor()
        create_table2 = '''CREATE TABLE workers_info (user_id INTEGER PRIMARY KEY, full_name text)'''
        cursor7.execute(create_table2)
except sqlite3.Error as error:
    # error.args ('table workers_info already exists',) is a system message when your DB already has table
    if error.args == ('table workers_info already exists',):
        print("Напоминаю, таблица работников уже создана", error)


class Form(StatesGroup):
    project = State()
    project_name = State()
    answer = State()
    final_answer = State()
    final_answer2 = State()
    end = State()
    first_message = State()


@dp.message_handler(commands=['set_projects'])
async def set_projects(message: types.Message):
    global chat_admins
    if message.from_user.id in chat_admins:
        await Form.project.set()
        await message.reply("Перечислите проекты через запятую без пробелов")
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
    temp = []
    i = 0
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
async def sen_polls(message: types.Message):
    # Attribute of function cant be removed cause without it function raises error
    if message.from_user.id in chat_admins:
        aioschedule.every().day.at('08:00').do(create_polls)
        aioschedule.every().day.at('09:00').do(checking)
        while True:
            await aioschedule.run_pending()
            await asyncio.sleep(1)


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
        await Form.final_answer.set()
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, selective=True)
        markup.add("Продолжить")
        await bot.send_message(call.user.id, "Нажмите продолжить", reply_markup=markup)


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
        now = (f'{datetime.datetime.now().year}-{datetime.datetime.now().month}-'
               f'{datetime.datetime.now().day}')
        data_tuple = (worker_daily_stat, now, questions[message.chat.id], message.chat.id, hours.replace(',', '.'))
        insert_into_db(data_tuple)
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


async def greetings(new_member_id):
    global group_id
    await bot.send_message(group_id, f"User_id {new_member_id} \n"
                                     f"Вас добавили в рабочую группу, откройте чат с ботом @FTE_tracker_bot"
                                     f" и нажмите кнопку 'Старт', чтобы он мог получать от вас данные")


@dp.message_handler(content_types=['new_chat_members'])
async def new_user_joined(message: types.Message):
    get_workers_names()
    global group_id
    if message.chat.id == group_id:
        for new_member in message.new_chat_members:
            if new_member.id not in workers_in_db:
                chat_members.append(new_member.id)
                await greetings(new_member.id)


@dp.message_handler(commands=['start'])
async def send_full_name(message: types.Message):
    await Form.first_message.set()
    await bot.send_message(message.chat.id, 'Напишите свое ФИО')


@dp.message_handler(state=Form.first_message)
async def first_message_to_bot(message: types.Message, state: FSMContext):
    get_workers_names()
    global workers_in_db
    if message.chat.id not in workers_in_db:
        data_tuple = (message.chat.id, message.text)
        add_new_member_to_workers_info(data_tuple)
        await bot.send_message(message.chat.id, 'Вы добавлены в базу')
        await state.finish()
    else:
        await state.finish()


if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
