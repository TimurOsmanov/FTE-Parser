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
bot_token, api_hash = '', ''
api_id, admin_id, group_id = 0, 0, 0
user_id = 0
polls, chosen_projects, question_index = {}, {}, {}
projects_set_by_admin, chat_members, chat_admins = [], [], []
bot = Bot(bot_token)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)


def get_chat_members():
    global chat_admins
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


def insert_into_db(data_tuple):
    # Function inserts new record according to new answer in poll
    with sqlite3.connect('sqlite_python.db') as conn3:
        cursor3 = conn3.cursor()
        insert_info = '''INSERT INTO FTE_info (unique_id, date, project,
        first_name, username, user_id, hours) VALUES (?, ?, ?, ?, ?, ?, ?)'''
        cursor3.execute(insert_info, data_tuple)


try:
    with sqlite3.connect('sqlite_python.db') as conn2:
        cursor2 = conn2.cursor()
        create_table = '''CREATE TABLE FTE_info (
                                unique_id INTEGER PRIMARY KEY,
                                date datetime,
                                project text,
                                first_name text,
                                username text,
                                user_id INTEGER,
                                hours REAL NOT NULL)'''
        cursor2.execute(create_table)
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


class Form(StatesGroup):
    project = State()
    project_name = State()
    answer = State()
    final_answer = State()


@dp.message_handler(commands=['set_projects'])
async def cmd_start(message: types.Message):
    if message.from_user.id == admin_id:
        await Form.project.set()
        await message.reply("Перечислите проекты через запятую без пробелов")
    else:
        await bot.send_message(message.chat.id, 'Нет прав')


@dp.message_handler(state=Form.project)
async def admin_set_projects(message: types.Message, state: FSMContext):
    global projects_set_by_admin
    projects_set_by_admin = []
    async with state.proxy() as data:
        data['project_name'] = message.text
    projects_set_by_admin = data['project_name'].split(',')
    await state.finish()


@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    # Attribute of function cant be removed cause without it function raises error
    await create_1st_poll()
    # aioschedule.every().day.at('').do(create_1st_poll)
    # while True:
    #     await aioschedule.run_pending()
    #     await asyncio.sleep(1)


async def create_1st_poll():
    global projects_set_by_admin, chat_members
    for user in chat_members:
        await bot.send_poll(user, 'В каких проектах вы принимали участие?',
                            projects_set_by_admin, is_anonymous=False, allows_multiple_answers=True)


@dp.poll_answer_handler()
async def voting(call):
    global chosen_projects, user_id, worker_daily_stat
    projects = []
    user_id = call.user.id
    for project_num in call.option_ids:
        projects.append(projects_set_by_admin[project_num])
    chosen_projects[user_id] = projects
    await Form.final_answer.set()
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, selective=True)
    markup.add("Продолжить")
    await bot.send_message(user_id, "Нажмите продолжить", reply_markup=markup)


@dp.message_handler(lambda message: message.text in ["Продолжить"], state=Form.final_answer)
async def create_questions_from_polls(message: types.Message, state: FSMContext):
    # Attribute of function cant be removed cause without it function raises error
    global user_id, question_index, chosen_projects
    question_index[user_id] = 0
    await Form.answer.set()
    try:
        project = chosen_projects[user_id][question_index[user_id]]
        await bot.send_message(user_id, f'Сколько вы работали над {project}?')
        polls[user_id] = project
        question_index[user_id] += 1
        await Form.final_answer.set()
    except IndexError:
        await bot.send_message(user_id, f'Произошла ошибка, обратитесь к администратору')
        await state.finish()


@dp.message_handler(lambda message: message.text not in ["Продолжить"], state=Form.final_answer)
async def create_questions_from_polls(message: types.Message, state: FSMContext):
    global user_id, question_index, worker_daily_stat, user_id, chosen_projects
    async with state.proxy() as data:
        data['answer'] = message.text
    hours = data['answer']
    worker_daily_stat += 1
    now = (f'{datetime.datetime.now().year}-{datetime.datetime.now().month}-'
           f'{datetime.datetime.now().day}')
    data_tuple = (worker_daily_stat, now, polls[user_id], user_id,
                  user_id, user_id, hours)
    insert_into_db(data_tuple)
    await Form.answer.set()
    try:
        project = chosen_projects[user_id][question_index[user_id]]
        await bot.send_message(user_id, f'Сколько часов вы работали над {project}?')
        polls[user_id] = project
        question_index[user_id] += 1
        await Form.final_answer.set()
    except IndexError:
        user_id = 0
        question_index = {}
        await state.finish()


if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
