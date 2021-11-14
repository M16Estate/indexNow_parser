import json
from wsgiref import headers

import pymysql
import requests

m16_site = pymysql.connect(user='clonestate', password='yY9oF4fW2smO8x', host='m16-estate.ru', database='m16')
m16_service = pymysql.connect(user='clonestate', password='yY9oF4fW2smO8x', host='m16-estate.ru',
                              database='m16_service')
new_local_db = {}
new_sub_local_db = {}
old_local_db = {}
final_local_db = {}

upd_local_db = {}
ins_local_db = {}

mod_counter = 0

with m16_site:
    cur = m16_site.cursor()
    cur.execute("SELECT id,date_update,slug,category_id FROM articles LIMIT 4")
    data = cur.fetchall()
    for datum in data:
        buf = {'timestamp': datum[1].strftime("%Y-%m-%d %H:%M:%S")}
        buf_2 = {'timestamp': datum[1].strftime("%Y-%m-%d %H:%M:%S")}
        if int(datum[3]) == 0:
            buf['link'] = 'https://m16-estate.ru/articles/' + datum[2]
            buf_2['link'] = 'https://m16-estate.ru/articles/' + datum[2]
        else:
            buf['link'] = 'https://m16-estate.ru/news/' + datum[2]
            buf_2['link'] = 'https://m16-estate.ru/news/' + datum[2]
        buf_2['slug'] = datum[2]
        buf_2['category_id'] = datum[3]
        new_local_db[int(datum[0])] = buf
        new_sub_local_db[int(datum[0])] = buf_2

with m16_service:
    cur = m16_service.cursor()
    cur.execute("SELECT id,date_update,slug,category_id FROM articles")
    data = cur.fetchall()
    for datum in data:
        buf = {'timestamp': datum[1].strftime("%Y-%m-%d %H:%M:%S")}
        if int(datum[3]) == 0:
            buf['link'] = 'https://m16-estate.ru/articles/' + datum[2]
        else:
            buf['link'] = 'https://m16-estate.ru/news/' + datum[2]
        old_local_db[int(datum[0])] = buf

for datum in new_local_db:
    if datum in old_local_db:
        if old_local_db[datum] == new_local_db[datum]:
            print("Page ID[" + str(datum) + "] is up to date;")
        else:
            mod_counter += 1
            print("Page ID[" + str(datum) + "] ready for updating;")
            final_local_db[datum] = new_local_db[datum]
            upd_local_db[datum] = new_sub_local_db[datum]
    else:
        mod_counter += 1
        print("Page ID[" + str(datum) + "] ready for inserting;")
        final_local_db[datum] = new_local_db[datum]
        ins_local_db[datum] = new_sub_local_db[datum]


post_json = {
    "host": "m16-estate.ru",
    "key": "aBoBaNOTaMoGus",
    "keyLocation": "https://m16-estate.ru/aBoBaNOTaMoGus.txt",
    "urlList": []
}

for datum in final_local_db:
    post_json['urlList'].append(final_local_db[datum]['link'])
post_json = json.dumps(post_json)

if mod_counter > 0:
    print("Sending pages (" + str(mod_counter) + ") to Yandex...")
    response = requests.post('https://yandex.com/indexnow', data=post_json)
    print(response)
    print('Setting up new and updated pages in registry...')
    m16_service = pymysql.connect(user='clonestate', password='yY9oF4fW2smO8x', host='m16-estate.ru',
                                  database='m16_service')
    with m16_service:
        cur = m16_service.cursor()
        if len(upd_local_db) > 0:
            for datum in upd_local_db:
                cur.execute("UPDATE articles SET date_update='" +
                            upd_local_db[datum]['timestamp'] + "',slug='" + upd_local_db[datum][
                                'slug'] + "' WHERE id=" + str(
                    datum))
                m16_service.commit()
        if len(ins_local_db) > 0:
            for datum in ins_local_db:
                cur.execute(
                    "INSERT INTO articles VALUES(" + str(datum) + ",'" + ins_local_db[datum]['timestamp'] + "','" +
                    ins_local_db[datum]['slug'] + "'," + str(
                        ins_local_db[datum]['category_id']) + ",CURRENT_TIMESTAMP)")
                m16_service.commit()
print('Finished!')
