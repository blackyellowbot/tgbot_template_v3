from typing import Union
from tgbot.config import Config
import asyncpg
from asyncpg import Pool, Connection


class Database:
    def __init__(self):
        self.pool = None

    def __int__(self):
        self.pool: Union[Pool] = None

    async def create(self):
        self.pool = Config.db

    async def execute(self, command, *args,
                      fetch: bool = False,
                      fetchval: bool = False,
                      fetchrow: bool = False,
                      execute: bool = False
                      ):
        async with self.pool.acquire() as connection:
            connection: Connection
            async with connection.transaction():
                if fetch:
                    result = await connection.fetch(command, *args)
                elif fetchval:
                    result = await connection.fetchval(command, *args)
                elif fetchrow:
                    result = await connection.fetchrow(command, *args)
                elif execute:
                    result = await connection.execute(command, *args)

            return result

    async def create_table_users(self):
        sql = """"CREATE TABLE IF NOT EXISTS users ('
                'id SERIAL PRIMARY KEY, '
                'user_id INTEGER, '
                'username VARCHAR(255), '
                'first_name VARCHAR(255), '
                'last_name VARCHAR(255))"""
        await self.execute(sql, execute=True)


