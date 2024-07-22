import logging
import vk_api
import sql
from psycopg2.errors import UniqueViolation
from psycopg2 import ProgrammingError

from data import USER_TOKEN, GROUP_ID, dbname, dbuser, dbpassword, dbhost, dbport


logging.basicConfig(level=logging.INFO)


class City:
    def __init__(self):
        self.id = None
        self.title = None

class GroupMember:
    def __init__(self):
        self.vk_id = None
        self.domain = None
        self.first_name = None
        self.last_name = None
        self.sex = None
        self.birth_date = None
        self.city_id = None


class Cities(sql.Table):
    name = 'cities'
    type = City
    fields = {
        'id': {'type': 'int'},
        'title': {}
    }

class GroupMembers(sql.Table):
    name = 'group_members'
    type = GroupMember
    fields = {
        'vk_id': {'type': 'int'},
        'domain': {},
        'first_name': {},
        'last_name': {},
        'sex': {},
        'birth_date': {},
    }
    joins = {
        'city_id': {'table': Cities, 'field': 'id'}
    }


class Database:
    def __init__(self):
        # Инициализируем объект vk для взаимодействия с API
        self.vk = vk_api.VkApi(token=USER_TOKEN).get_api()

        # Подключаемся к базе данных
        sql.db = sql.Db(f'dbname={dbname} user={dbuser} password={dbpassword} host={dbhost} port={dbport}')
        sql.Table.schema = 'public'
        sql.query("""
            CREATE TABLE IF NOT EXISTS cities (
                id INT PRIMARY KEY,
                title TEXT NOT NULL
            )""")
        sql.query("""
            CREATE TABLE IF NOT EXISTS group_members (
                vk_id BIGINT PRIMARY KEY,
                domain TEXT NOT NULL UNIQUE,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                sex CHAR CHECK (sex::TEXT = 'м'::TEXT OR sex::TEXT = 'ж'::TEXT),
                birth_date DATE,
                city_id INT REFERENCES cities (id)
            )""")

    def generate(self) -> None:
        self.__clear()
        group_members = self.__get_group_members()

        for member in group_members:
            city_id = "NULL"
            if 'city' in member:
                city_id = self.__add_city(member['city'])

            info_to_add = {
                'vk_id': member['id'],
                'domain': member['domain'],
                'first_name': member['first_name'],
                'last_name': member['last_name'],
                'sex': "'ж'" if member['sex'] == 1 else "'м'" if member['sex'] == 2 else "NULL",
                'birth_date': f"'{self.__get_birth_date(member['bdate'])}'" if 'bdate' in member else "NULL",
                'city_id': city_id,
            }

            # GroupMembers.add(info_to_add)
            try:
                sql.query(f"INSERT INTO group_members VALUES ('{info_to_add['vk_id']}', '{info_to_add['domain']}', "
                          f"'{info_to_add['first_name']}', '{info_to_add['last_name']}', {info_to_add['sex']}, "
                          f"{info_to_add['birth_date']}, {info_to_add['city_id']});")
            except ProgrammingError:
                pass

    def add_new_people(self) -> None:
        new_members = self.__get_group_members(new=True)
        for member in new_members:
            # Если попадается человек, который уже есть в бд, останавливаемся
            if sql.query(f"SELECT 1 FROM group_members WHERE vk_id = '{member['id']}';"):
                break

            sex = "'ж'" if member['sex'] == 1 else "'м'" if member['sex'] == 2 else "NULL"
            birth_date = f"'{self.__get_birth_date(member['bdate'])}'" if 'bdate' in member else "NULL"
            city_id = None
            if 'city' in member:
                city_id = self.__add_city(member['city'])
            try:
                sql.query(f"INSERT INTO group_members VALUES ('{member['vk_id']}', '{member['domain']}', "
                          f"'{member['first_name']}', '{member['last_name']}', {sex}, {birth_date}, {city_id});")
            except ProgrammingError:
                pass

    def __get_group_members(self, new: bool=False) -> list[dict]:
        offset = 0
        group_members = []
        fields_to_return = [
            'bdate',
            'city',
            'domain',
            'education',
            'sex',
        ]
        while True:
            result = self.vk.groups.getMembers(
                group_id=GROUP_ID,
                sort='time_desc',
                fields=','.join(fields_to_return),
                offset=offset
            )
            group_members.extend(result['items'])
            offset += 1000
            if result['count'] <= offset or new:
                break
        return group_members

    @staticmethod
    def __get_birth_date(bdate: str) -> str:
        birth_data = bdate.split('.')
        day = birth_data[0]
        month = birth_data[1]
        if len(birth_data) == 3:
            year = birth_data[2]
        else:
            year = 1000
        return f'{year}-{month}-{day}'

    @staticmethod
    def __clear():
        try:
            sql.query("DELETE FROM group_members;")
        except ProgrammingError:
            pass
        try:
            sql.query("DELETE FROM cities;")
        except ProgrammingError:
            pass

    @staticmethod
    def __add_city(city_info: dict) -> int:
        try:
            Cities.add(city_info)
        except UniqueViolation:
            pass
        except Exception as e:
            logging.error(f'unable to add city "{city_info["title"]}" ({city_info["id"]}): {e}')
            pass
        return city_info['id']