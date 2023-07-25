import time
import datetime as dt
from pprint import pprint

from pytz import utc, timezone
import requests
from config import *
from my_logger import get_logger
logger = get_logger("app.newlist")


def send_mixin(msg, _type="PLAIN_TEXT"):
    token = MIXIN_TOKEN
    url = f"https://webhook.exinwork.com/api/send?access_token={token}"

    msg = f"【{RUN_NAME}】\n\n" + msg
    value = {
        'category': _type,
        'data': msg,
    }

    try:
        r = requests.post(url, data=value, timeout=2).json()
    except Exception as err:
        logger.error(f"Send Mixin Error: {err}")
        logger.exception(err)


def fetch_new_list_by_page(date:str="", page:int=1):
    """
    根据当天时间戳+页码，获取某一页的new_list，返回总页数、当前页码、new_list
    :param date: utc当天0点的10位时间戳，string
    :param page: 页码
    :return: total_pages, page, content
    """
    url = "https://sapi.coincarp.com/api/v1/news/calendar/index?tagcode=exchange&timestamp={date}&page={page}&pagesize=100&lang=zh-CN&type="
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
        "Referer": "https://www.coincarp.com/",
        "Origin": "https://www.coincarp.com",
        "Sec-Ch-Ua-Platform": '"Windows"',
    }

    url = url.format(date=date, page=page)
    logger.debug(url)

    try:
        res = requests.get(url=url, headers=headers).json()

        if res["code"] == 200:
            data = res["data"]
            total_pages = data["total_pages"]
            page = data["page"]
            content = data["list"]

            logger.debug(data)
            return total_pages, page, content
        else:
            logger.error(res["msg"])
            return False
    except Exception as err:
        logger.exception(err)
        return False


def fetch_new_list_all(date:str=""):
    """
    根据日期来获取当时的上币列表
    :param date: 日期字符串”2023-08-24“，如果为空则回传当天的上币列表
    :return:
    """
    if date == "":
        # utc当天0点的10位时间戳
        date = int(dt.datetime.now(tz=utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    else:
        date = int(utc.localize(dt.datetime.strptime(date, "%Y-%m-%d")).timestamp())

    total_pages, page, content = fetch_new_list_by_page(date=date, page=1)
    if total_pages > 1:
        for p in range(page+1, total_pages+1):
            _, _, _content = fetch_new_list_by_page(date=date, page=p)
            content += _content
            time.sleep(0.5)

    logger.debug(content)
    return content


def get_monitored_list(new_list):
    """
    如果有关注的交易所上新市价发生，就返回所有关注的事件
    :param new_list: fetch_new_list_all获取到的所有上新事件
    :return: 关注的上新事件list
    """
    events = []
    for i in new_list:
        for j in i["eventlist"]:
            site = j["eventcode"].split("on-")[-1]
            if site in Monitored_Sites:
                events.append(j)

    return events


def send_new_list(events):
    """
    将所有关注的上新事件格式化后发送到Mixin
    :param events: get_monitored_list return
    :return:
    """
    msg = ""
    for e in events:
        msg += f'{e["nativename"]}\n'
        msg += f'{e["description"]}\n'

    send_mixin(msg)


def main():

    # 获取近期上币的json
    new_list = fetch_new_list_all("2023-07-24")

    # 获取关注交易所的上币信息，Monitored_Sites
    events = get_monitored_list(new_list)

    # 发送结果
    if events:
        send_new_list(events)


if __name__ == '__main__':
    main()
