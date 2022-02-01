import config
import urllib3
import xmltodict
import psycopg2
import re
from PIL import Image
import requests
from io import BytesIO
from datetime import datetime
import schedule as sh
import time


def test():
    print("Test work")
    print(datetime.now())


def job():
    start_time = datetime.now()
    for user in config.users:
        index_arr = []
        city_arr = []
        category_arr = []
        posts = []
        posts_crm_ids = []
        print("I'm working...")
        url = user["url"]
        http = urllib3.PoolManager()
        response = http.request('GET', url)
        data = xmltodict.parse(response.data)
        ADS = data["Ads"]
        AD = ADS["Ad"]
        for post in AD:
            post_dict = {}
            for rows_index in post:
                if type(post[rows_index]) == str:
                    post_dict[rows_index.lower()] = post[rows_index]
                else:
                    if rows_index.lower() != "images" and post[rows_index] is not None:
                        try:
                            subs = []
                            for col_i in post[rows_index]:
                                if type(post[rows_index][col_i] == str):
                                    subs.append({col_i.lower(): post[rows_index][col_i]})
                            post_dict[rows_index.lower()] = subs
                        except Exception:
                            print('err')
                if rows_index.lower() == "images" and post[rows_index] is not None:
                    try:
                        images = post[rows_index]
                        for image in images:
                            if type(images[image]) == list:
                                image_list = images[image]
                                subs = []
                                for img in image_list:
                                    try:
                                        for i in img:
                                            if i.lower() == "@url":
                                                PHOTO = img[i]
                                                subs.append(PHOTO)
                                    except Exception:
                                        print("err")
                                post_dict[rows_index.lower()] = subs
                            else:
                                img = images[image]
                                try:
                                    subs = []
                                    for i in img:
                                        if i.lower() == "@url":
                                            PHOTO = img[i]
                                            subs.append(PHOTO)
                                    post_dict[rows_index.lower()] = subs
                                except Exception:
                                    print("err")
                    except Exception:
                        print("err")

                if rows_index == "City":
                    city_arr.append(post[rows_index])
                if rows_index == "Category":
                    category_arr.append(post[rows_index])
                index_arr.append(rows_index)
            try:
                post_dict['images']
            except Exception:
                post_dict['images'] = [config.default_photo_nedvizh]
            posts.append(post_dict)

        print("\n***   ***   ***\n")
        print("Всего объявлений: ", len(AD))
        print(set(city_arr))
        print(set(index_arr))
        print("END")
        print("\n***   ***   ***\n")

        posts = {"posts": posts}

        db_posts = []
        for post in posts["posts"]:
            try:
                # Проверка региона и города для других компаний
                manager_phone = re.sub("\D", "", post["contactphone"])
                if len(manager_phone) != 11:
                    manager_phone = "+7" + str(manager_phone)
                db_post = {
                    "user_id": user["id"],
                    "crm_id": post["id"],
                    "title": None,
                    "manager_name": post["managername"],
                    "price": int(post["price"]),
                    "trade": False,
                    "bymessages": True,
                    "byphone": True,
                    "coordinates": "[\"" + str(post["latitude"]) + "\",\"" + str(post["longitude"]) + "\"]",
                    "manager_phone": manager_phone,
                    "contact": manager_phone,
                    "description": post["description"],
                    "location": post["address"],
                    "city": "RU$RU-CHE$Челябинск",
                    "subcategory": None,
                    "alias": None,
                    "additional_fields": [],
                    "photo_url": post["images"]
                    }
                if post["category"].lower() in ["комнаты", "коммерческая недвижимость", "квартиры", "земельные участки",
                                                "дома, дачи, коттеджи", "гаражи и машиноместа"]:
                    # Выкладка
                    indexses = [
                        "Status", "Decoration", "ObjectType", "WallsType", "DistanceToCity", "Square", "CadastralNumber",
                        "MarketType", "DealType", "ObjectSubtype", "HouseType", "OperationType", "Rooms", "District",
                        "NewDevelopmentId", 'Secured", "PropertyRights", "Category", "BalconyOrLoggia', "Floor", "Floors",
                        "LandArea", "ApartmentNumber", "RoomType"]

                    if post["category"].lower() == "квартиры" and post['operationtype'].lower() == "продам":
                        db_post["subcategory"] = "sell_apartments"
                        db_post["title"] = str(post["rooms"]) + "-к. квартира, " + post["square"] + " м², " + post["floor"] + "/" + post["floors"] + " эт."
                        db_post["alias"] = "real_estate,apartments_kv,sell_apartments"
                        additional_fields = []
                        post_keys = []
                        for key, value in post.items():
                            post_keys.append(key)
                        post_keys = list(set(post_keys) - {"id", "managername", "street", "videourl", "price", "longitude", "latitude", "city", "category", "address", "region", "city", "district", "description", "kitchenspace", "contactphone", "images", "safedemonstration"})
                        for ii in post_keys:
                            if ii == "cadastralnumber":
                                additional_fields.append({"alias": "cadastral_number", "value": post[ii]})
                            if ii == "floor":
                                additional_fields.append({"alias": "floor", "value": post[ii]})
                            if ii == "square":
                                additional_fields.append({"alias": "square", "value": post[ii]})
                            if ii == "propertyrights":
                                additional_fields.append({"alias": "ad_owner", "value": post[ii]})
                            if ii == "balconyorloggia":
                                additional_fields.append({"alias": "balcony_or_loggia", "value": post[ii]})
                            if ii == "decoration":
                                additional_fields.append({"alias": "finishing", "value": post[ii]})
                            if ii == "floors":
                                additional_fields.append({"alias": "number_of_storeys_of_the_house", "value": post[ii]})
                            if ii == "rooms":
                                additional_fields.append({"alias": "number_of_rooms", "value": post[ii]})
                            if ii == "markettype":
                                additional_fields.append({"alias": "resale_new_building", "value": post[ii]})
                            if ii == "housetype":
                                additional_fields.append({"alias": "material", "value": post[ii]})
                        db_post["additional_fields"] = additional_fields
                    if post["category"].lower() == "комнаты" and post['operationtype'].lower() == "продам":
                        db_post["subcategory"] = "sell_rooms"
                        db_post["title"] = "Комната " + post["square"] + " м², " + post["floor"] + "/" + post["floors"] + " эт."
                        db_post["alias"] = "real_estate,rooms,sell_rooms"
                        additional_fields = []
                        post_keys = []
                        for key, value in post.items():
                            post_keys.append(key)
                        post_keys = list(set(post_keys) - {"id", "managername", "street", "videourl", "price","longitude", "latitude", "city", "category", "address", "region", "city", "district", "description", "kitchenspace", "contactphone", "images", "safedemonstration"})
                        for ii in post_keys:
                            if ii == "floors":
                                additional_fields.append({"alias": "number_of_storeys_of_the_house", "value": post[ii]})
                            if ii == "propertyrights":
                                additional_fields.append({"alias": "ad_owner", "value": post[ii]})
                            if ii == "rooms":
                                additional_fields.append({"alias": "number_of_rooms_in_the_apartment", "value": post[ii]})
                            if ii == "square":
                                additional_fields.append({"alias": "square", "value": post[ii]})
                            if ii == "floor":
                                additional_fields.append({"alias": "floor", "value": post[ii]})
                            if ii == "housetype":
                                additional_fields.append({"alias": "material", "value": post[ii]})
                        db_post["additional_fields"] = additional_fields
                    if post["category"].lower() == "коммерческая недвижимость" and post['operationtype'].lower() == "продам":
                        db_post["subcategory"] = "sell_commercial_property"
                        db_post["title"] = str(post["operationtype"]) + " " + post["objecttype"].lower() + ", " + post["square"] + " м²"
                        db_post["alias"] = "real_estate,commercial_property_second,sell_commercial_property"
                        additional_fields = []
                        post_keys = []
                        for key, value in post.items():
                            post_keys.append(key)
                        post_keys = list(set(post_keys) - {"id", "managername", "street", "videourl", "price", "longitude", "latitude", "city", "category", "address", "region", "city", "district", "description", "kitchenspace", "contactphone", "images", "safedemonstration"})
                        for ii in post_keys:
                            if ii == "objecttype":
                                additional_fields.append({"alias": "property_type", "value": post[ii]})
                            if ii == "propertyrights":
                                additional_fields.append({"alias": "ad_owner", "value": post[ii]})
                            if ii == "square":
                                additional_fields.append({"alias": "square", "value": post[ii]})
                        db_post["additional_fields"] = additional_fields

                    if post["category"].lower() == "земельные участки" and post['operationtype'].lower() == "продам":
                        db_post["subcategory"] = "sell_land"
                        db_post["title"] = "Участок " + post["landarea"] + " сот."
                        db_post["alias"] = "real_estate,land,sell_land"
                        additional_fields = []
                        post_keys = []
                        for key, value in post.items():
                            post_keys.append(key)
                        post_keys = list(set(post_keys) - {"id", "managername", "street", "videourl", "price", "longitude", "latitude", "city", "category", "address", "region", "city", "district", "description", "kitchenspace", "contactphone", "images", "safedemonstration"})
                        for ii in post_keys:
                            if ii == "propertyrights":
                                additional_fields.append({"alias": "ad_owner", "value": post[ii]})
                            if ii == "landarea":
                                additional_fields.append({"alias": "square", "value": post[ii]})
                            if ii == "distancetocity":
                                additional_fields.append({"alias": "distance_to_city", "value": post[ii]})
                        db_post["additional_fields"] = additional_fields
                    if post["category"].lower() == "дома, дачи, коттеджи" and post['operationtype'].lower() == "продам":
                        db_post["subcategory"] = "sell_houses_and_cottages"
                        db_post["title"] = str(post["objecttype"]) + " " + post["square"] + " м² на участке " + post["landarea"] + " сот."
                        db_post["alias"] = "real_estate,houses_and_cottages,sell_houses_and_cottages"
                        additional_fields = []
                        post_keys = []
                        for key, value in post.items():
                            post_keys.append(key)
                        post_keys = list(
                            set(post_keys) - {"id", "managername", "street", "videourl", "price", "longitude", "latitude", "city", "category", "address", "region", "city", "district", "description", "kitchenspace", "contactphone", "images", "safedemonstration"})
                        for ii in post_keys:
                            if ii == "landarea":
                                additional_fields.append({"alias": "land_area", "value": post[ii]})
                            if ii == "wallstype":
                                additional_fields.append({"alias": "wall_material", "value": post[ii]})
                            if ii == "floors":
                                additional_fields.append({"alias": "number_of_storeys", "value": post[ii]})
                            if ii == "square":
                                additional_fields.append({"alias": "house_area", "value": post[ii]})
                            if ii == "objecttype":
                                additional_fields.append({"alias": "house_type", "value": post[ii]})
                            if ii == "propertyrights":
                                additional_fields.append({"alias": "ad_owner", "value": post[ii]})
                        db_post["additional_fields"] = additional_fields
                    if post["category"].lower() == "гаражи и машиноместа" and post['operationtype'].lower() == "продам":
                        db_post["subcategory"] = "sell_garages_and_parking_spaces_second"
                        db_post["title"] = str(post["objecttype"]) + " " + post["square"] + " м²"
                        db_post["alias"] = "real_estate,garages_and_parking_spaces_second,sell_garages_and_parking_spaces_second"
                        additional_fields = []
                        post_keys = []
                        for key, value in post.items():
                            post_keys.append(key)
                        post_keys = list(
                            set(post_keys) - {"id", "managername", "street", "videourl", "price", "longitude", "latitude", "city", "category", "address", "region", "city", "district", "description", "kitchenspace", "contactphone", "images", "safedemonstration"})
                        for ii in post_keys:
                            if ii == "propertyrights":
                                additional_fields.append({"alias": "ad_owner", "value": post[ii]})
                            if ii == "square":
                                additional_fields.append({"alias": "square", "value": post[ii]})
                            if ii == "objectsubtype":
                                additional_fields.append({"alias": "garage_parking_space", "value": post[ii]})
                        db_post["additional_fields"] = additional_fields
                    db_posts.append(db_post)
                posts_crm_ids.append(post["id"])
            except Exception as e:
                print("AliasError:  ", e)
        print("_++_+_+_+_+_+_+_+")
        print(len(db_posts))
        print(set(category_arr))
        print(posts_crm_ids)
        print("_++_+_+_+_+_+_+_+")

        try:
            con = psycopg2.connect(database=config.db_database, user=config.db_user, password=config.db_password, host=config.db_host, port=config.db_port)
            cur = con.cursor()
            cur.execute('SELECT array_to_json(array_agg(row_to_json(t)))from (SELECT "id", "crm_id" FROM "public"."posts" WHERE "posts"."crm_id" IS NOT NULL AND user_id = ' + str(user["id"]) + ") t")
            results = cur.fetchall()[0][0]
            con.close()
            exist_posts_crm_ids = []
            if results is None:
                results = []
            if len(results) > 0:
                for zz in results:
                    exist_posts_crm_ids.append(zz["crm_id"])
            print("~~~~~~~~~~~")
            posts_for_upload = [x for x in posts_crm_ids if x not in exist_posts_crm_ids]
            posts_for_delete = [x for x in exist_posts_crm_ids if x not in posts_crm_ids]
            posts_for_update = list(set(posts_crm_ids) & set(exist_posts_crm_ids))
            print(posts_for_upload)
            print(posts_for_delete)
            print(posts_for_update)

            if len(posts_for_delete) > 0:
                con = psycopg2.connect(database=config.db_database, user=config.db_user, password=config.db_password, host=config.db_host, port=config.db_port)
                cur = con.cursor()
                sql = 'DELETE from "public"."posts" WHERE "posts"."user_id" = ' + str(user["id"]) + ' AND crm_id IN %s'
                cur.execute(sql, (tuple(posts_for_delete),))
                con.commit()
                con.close()

            for ix in db_posts:
                print("---   ---   ---")
                if ix["crm_id"] in posts_for_upload:
                    print("upload")
                    headers = {'x-access-token': user["token"]}
                    r = requests.post(str(config.server_url) + "setPosts", headers=headers, json=ix)
                    post_id = r.json()["id"]
                    print(post_id)
                    files = []
                    for i in ix["photo_url"]:
                        response = requests.get(i)
                        img = Image.open(BytesIO(response.content))
                        img = img.convert("RGB")
                        buf = BytesIO()
                        img.save(buf, 'jpeg')
                        buf.seek(0)
                        image_bytes = buf.read()
                        files.append(('files[]', ("None.webp", image_bytes)))
                    headers = {'x-access-token': user["token"]}
                    r = requests.post(config.images_server_url + str(user["id"]) + "/" + str(post_id), headers=headers, files=files)
                    print(r.status_code)
                if ix["crm_id"] in posts_for_update:
                    print("update")
                    kvik_post_id = [item for item in results if item["crm_id"] == ix["crm_id"]][0]['id']
                    ix["post_id"] = kvik_post_id
                    headers = {'x-access-token': user["token"]}
                    r = requests.post(str(config.server_url) + "updateFull", headers=headers, json=ix)
                    post_id = r.json()["id"]
                    print(post_id)
                    files = []
                    for i in ix["photo_url"]:
                        response = requests.get(i)
                        img = Image.open(BytesIO(response.content))
                        img = img.convert("RGB")
                        buf = BytesIO()
                        img.save(buf, 'jpeg')
                        buf.seek(0)
                        image_bytes = buf.read()
                        files.append(('files[]', ("None.webp", image_bytes)))
                    headers = {'x-access-token': user["token"]}
                    r = requests.post(config.images_server_url + str(user["id"]) + "/" + str(post_id),
                                      headers=headers, files=files)
                    print(r.status_code)

        except Exception as e:
            print(e)
    print("Time: ", datetime.now() - start_time)


sh.every(30).seconds.do(test)
sh.every().day.at("00:00").do(job)


while 1:
    sh.run_pending()
    time.sleep(1)
