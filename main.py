import sqlite3
import pandas as pd
pd.set_option('display.max_rows', None)

# создаем базу данных и устанавливаем соединение с ней
con = sqlite3.connect("book_store.sqlite")

# открываем файл с дампом базой данных
f_damp = open('store.db', 'r', encoding='utf-8-sig')

# читаем данные из файла
damp = f_damp.read()
# закрываем файл с дампом
f_damp.close()

# создаем курсор
cursor = con.cursor()

# Запрос1
'''Вывести названия, авторов и жанры тех книг, количество больше 4, а цена либо меньше 200 рублей,
    либо больше 400 рублей. Информацию отсортировать сначала по названиям книг,
    а затем по фамилиям авторов в алфавитном порядке.'''
cursor.execute('''
    SELECT title, name_author, name_genre
    FROM book
    JOIN genre ON (book.genre_id = genre.genre_id)
    JOIN author ON (book.author_id = author.author_id)
    WHERE (amount > 4) AND (price < 200 OR price > 400)
    ORDER BY 1,2''')
print(cursor.fetchall())

# Запрос2
'''Вывести фамилии клиентов, (без повторений) в заказах которых есть хотя бы одна книга,
заказанная в двух и более экземплярах. Информацию отсортировать по фамилии клиентов в алфавитном порядке.'''
cursor.execute('''
    SELECT DISTINCT name_client
    FROM client
    JOIN buy_book ON (buy_book.buy_id = buy.buy_id)
    JOIN buy ON (client.client_id = buy.client_id)
    WHERE amount >= 2
    ORDER BY 1''')
print(cursor.fetchall())


# Запрос3
'''Вывести название различных книг и их авторов, заказанных покупателями, проживающих в городе (городах),
    в котором (ых) проживает больше всего клиентов магазина, сделавших хотя бы один заказ.'''
#Находим всех клиентов, сделавших хотя бы один заказ(таблица заказчиков)
#Находим город с максимальным количеством повторений в таблице заказчиков
#Находим ID городов, количество повторений в таблице заказчиков, которых равно максимальному
#Выводим название различных книг и их авторов по ID, найденных выше городов
df = pd.read_sql('''
    WITH customers AS (
      SELECT DISTINCT client_id FROM buy
    ),
    city_max_repeat_count AS (
      SELECT COUNT(city_id) AS rezult
        FROM customers JOIN client ON (customers.client_id = client.client_id)
        GROUP BY city_id
        ORDER BY COUNT(*) DESC
        LIMIT 1
    ),
    found_cities AS (
      SELECT city_id AS rezult
      FROM customers JOIN client ON (customers.client_id = client.client_id), city_max_repeat_count
      GROUP BY city_id
      HAVING COUNT(city_id) = city_max_repeat_count.rezult
    )
    SELECT DISTINCT title, name_author
    FROM book
    JOIN author ON (book.author_id = author.author_id)
    JOIN buy_book ON (book.book_id = buy_book.book_id)
    JOIN buy ON (buy_book.buy_id = buy.buy_id)
    JOIN client ON (client.client_id = buy.client_id)
    WHERE client.city_id IN found_cities
''', con)
print(df)

# Запрос4
'''Поднять на 15% цену тех книг, которые хотя бы один раз заказывали покупатели (изменение внести в базу).'''
cursor.execute('''
    UPDATE book SET price = ROUND(price * 1.15,2) WHERE book_id IN(SELECT DISTINCT book_id FROM buy_book)
''')
#con.commit()#для сохранение изменений данных в БД

# Запрос5
'''Для каждой книги вывести количество книг,
    написанных ее автором, цена которых отличается от цены этой книги на 40 рублей и менее как в большую,
    так и меньшую стороны. Вывести автора, название книги, ее цену и количество книг,
    удовлетворяющих указанному требованию. Последний столбец назвать Количество.
    Примечание. Для решения задания № 5 использовать оконные функции.'''
#создаём запрос для получения книг одного автора разница в стоимости которых меньше или равна 40
#
df = pd.read_sql('''
  WITH help AS(
    SELECT b1.book_id, b1.title, b1.author_id, b1.price, b2.book_id, b2.title, b2.author_id, b2.price
    FROM book b1
    JOIN book b2 ON b1.author_id = b2.author_id
    WHERE ABS(b1.price - b2.price) <= 40 AND (b1.author_id = b2.author_id)
  )
  SELECT DISTINCT name_author, title, price,
  COUNT(*) OVER (PARTITION BY help.book_id ORDER BY help.price) - 1 AS Количество
  FROM help JOIN author ON help.author_id = author.author_id
  ORDER BY 1, 3
''', con)
print(df)
# закрываем соединение с базой
con.close()
