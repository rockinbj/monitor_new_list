import time
import datetime as dt
from pathlib import Path

import pandas as pd
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


def fetch_new_list_by_page(date: str = "", page: int = 1):
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
    logger.debug(f"url: {url}")

    try:
        res = requests.get(url=url, headers=headers).json()

        if res["code"] == 200:
            data = res["data"]
            total_pages = data["total_pages"]
            page = data["page"]
            content = data["list"]

            # logger.debug(data)
            return total_pages, page, content
        else:
            # logger.error(res["msg"])
            return False
    except Exception as err:
        logger.exception(err)
        return False


def fetch_new_list_all(date: str = ""):
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
        for p in range(page + 1, total_pages + 1):
            _, _, _content = fetch_new_list_by_page(date=date, page=p)
            content += _content
            time.sleep(0.5)

    # logger.debug(content)
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


def record_event(event, file_record: Path, send_timestamp=None):
    """
    将事件存储到记录文件中，增加一个10位的发送时间戳字符串
    :param event: get_monitored_list返回的events list中的event个体
    :param file_record: file_record, Path对象
    :param send_time: utc时间的10位时间戳，可以在发送前做记录
    :return: 返回file_record的Path对象
    """
    df_e = pd.DataFrame(event)
    df_e["send_time"] = int(send_timestamp)  # 1679944000
    if not file_record.exists():
        df_e.to_csv(file_record, encoding="gbk", index=False)
    else:
        df_e.to_csv(file_record, encoding="gbk", index=False, header=False, mode="a")

    return file_record


def load_events_record(file_record: Path):
    if file_record.exists():
        df_e = pd.read_csv(file_record, encoding="gbk")
        return df_e
    else:
        return None


def get_sent_history_count(event: dict, file_record: Path):
    event_name = event["eventcode"]
    df_e = load_events_record(file_record)

    if df_e is not None:
        sent_count = df_e["eventcode"].value_counts()[event_name]
        return sent_count
    else:
        return 0


def send_new_list(events, file_record: Path):
    """
    将所有关注的上新事件格式化后发送到Mixin，
    会判断每个事件的已发送次数，如果已经发送超过Repeat次数，就停止发送
    :param file_record: 存储事件df的csv文件Path对象
    :param events: get_monitored_list return
    :return:
    """
    msg = ""
    for e in events:
        if get_sent_history_count(event=e, file_record=file_record) >= Repeat:
            logger.debug(f"该事件已经发送过3次，不再发送")
            continue
        else:
            msg += f'{e["nativename"]}\n'
            msg += f'{e["description"]}\n\n'
            record_event(event=e, file_record=file_record, send_timestamp=dt.datetime.utcnow().timestamp())
            logger.debug(f"即将发送事件：{e['eventcode']}")

    if msg: send_mixin(msg)


def main():
    path_root = Path(__file__).resolve().parent
    path_data = path_root / "data"
    path_data.mkdir(parents=True, exist_ok=True)

    file_record = path_data / "announce_record.csv"

    # 获取近期上币的json
    logger.info(f"扫描日期：{Check_Date if Check_Date else '当天'}，开始扫描……")
    new_list = fetch_new_list_all(Check_Date)
    logger.info(f"共扫描到未来 {len(new_list)} 天的上新事件")
    logger.debug(f"所有的上新事件：{new_list}")

    # 获取关注交易所的上币信息，Monitored_Sites
    events = get_monitored_list(new_list)
    logger.info(f"关注的交易所 上新事件 {len(events)}")
    logger.debug(f"关注的上新事件：{new_list}")

    # 发送结果，并且做记录
    if events:
        send_new_list(events, file_record)
        logger.info(f"发送完成")


if __name__ == '__main__':
    main()
