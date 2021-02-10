import codecs
import datetime
import email
import email.parser
import fnmatch
import gzip
import logging
import mailbox
import os
import re
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import warnings
from email.header import Header
from email.message import Message
from email.mime.text import MIMEText
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd
import yaml
from bs4 import BeautifulSoup
from validator_collection import checkers

import bigbang.listserv as listserv


class ListservMessageWarning(BaseException):
    """Base class for Archive class specific exceptions"""

    pass


class ListservListWarning(BaseException):
    """Base class for Archive class specific exceptions"""

    pass


class ListservArchiveWarning(BaseException):
    """Base class for Archive class specific exceptions"""

    pass


class ListservMessage:
    """
    Parameters
    ----------

    Methods
    -------
    from_url
    get_body
    get_header
    to_dict
    """

    def __init__(
        self,
        body: str,
        Subject: str,
        FromName: str,
        FromAddr: str,
        ToName: str,
        ToAddr: str,
        Date: str,
        ContentType: str,
    ):
        self.Body = body
        self.Subject = Subject
        self.FromName = FromName
        self.FromAddr = FromAddr
        self.ToName = ToName
        self.ToAddr = ToAddr
        self.Date = Date
        self.ContentType = ContentType

    @classmethod
    def from_url(
        cls,
        list_name: str,
        url: str,
        fields: str = "total",
    ) -> "ListservMessage":
        """
        Args:
        """
        # TODO implement field selection, e.g. return only header, body, etc.
        soup = get_website_content(url)
        if fields in ["header", "total"]:
            header = ListservMessage.get_header(soup)
        else:
            header = {
                "Subject": None,
                "FromName": None,
                "FromAddr": None,
                "ToName": None,
                "ToAddr": None,
                "Date": None,
                "ContentType": None,
            }
        if fields in ["body", "total"]:
            body = ListservMessage.get_body(list_name, url, soup)
        else:
            body = None
        return cls(body, **header)

    @staticmethod
    def get_body(list_name: str, url: str, soup: BeautifulSoup) -> str:
        """"""
        url_root = ("/").join(url.split("/")[:-2])
        a_tags = soup.select(f'a[href*="A3="][href*="{list_name}"]')
        href_plain_text = [
            tag.get("href") for tag in a_tags if "Fplain" in tag.get("href")
        ][0]
        body_soup = get_website_content(
            urllib.parse.urljoin(url_root, href_plain_text)
        )
        return body_soup.find("pre").text

    @staticmethod
    def get_header(soup: BeautifulSoup) -> Dict[str, str]:
        """"""
        text = soup.find(
            "b",
            text=re.compile(r"^\bSubject\b"),
        ).parent.parent.parent.parent.text

        header = {}
        for field in text.split("Parts/Attachments:")[0].splitlines():
            if len(field) == 0:
                continue
            field_name = field.split(":")[0].strip()
            field_body = field.replace(field_name + ":", "").strip()
            header[field_name] = field_body

        header["FromName"] = listserv.get_name(header["From"]).strip()
        header["FromAddr"] = listserv.get_addr(header["From"])
        header["ToName"] = listserv.get_name(header["Reply-To"]).strip()
        header["ToAddr"] = listserv.get_addr(header["Reply-To"])
        header["Date"] = listserv.get_date(header["Date"])
        header["ContentType"] = header["Content-Type"]
        del header["From"], header["Reply-To"], header["Content-Type"]
        return header

    def to_dict(self) -> Dict[str, str]:
        dic = {
            "Body": self.Body,
            "Subject": self.Subject,
            "FromName": self.FromName,
            "FromAddr": self.FromAddr,
            "ToName": self.ToName,
            "ToAddr": self.ToAddr,
            "Date": self.Date,
            "ContentType": self.ContentType,
        }
        return dic


class ListservList:
    """
    This class handles a single mailing list of a public archive in the
    LISTSERV 16.5 format.

    Parameters
    ----------
    name
        The of whom the list (e.g. 3GPP_COMMON_IMS_XFER, IEEESCO-DIFUSION, ...)
    url
        The URL where the list lives

    Methods
    -------
    from_url
    to_dataframe
    yield_period
    yield_message
    to_dict
    to_pandas_dataframe

    Example
    -------
    mlist = ListservList.from_url(
        "3GPP_TSG_CT_WG6",
        url="https://list.etsi.org/scripts/wa.exe?A0=3GPP_TSG_CT_WG6",
        select={
            "years": (2020, 2021),
            "months": "January",
            "weeks": [1,5],
            "fields": "header",
        },
    )
    """

    def __init__(self, name: str, url: str, msgs: List[ListservMessage]):
        self.name = name
        self.url = url
        self.url_root = ("/").join(url.split("/")[:-2])
        self.messages = msgs

    def __len__(self) -> int:
        return len(self.messages)

    def __iter__(self):
        return iter(self.messages)

    def __getitem__(self, index) -> ListservMessage:
        return self.messages[index]

    @classmethod
    def from_url(
        cls,
        name: str,
        url: str,
        select: dict,
    ) -> "ListservList":
        """
        Args:
            name:
            url:
            select:
            datatype:
        """
        if "fields" not in list(select.keys()):
            select["fields"] = "total"
        msgs = cls.get_messages(name, url, select)
        return cls.from_messages(name, url, msgs)

    @classmethod
    def from_messages(
        cls,
        name: str,
        url: str,
        messages: Union[List[str], List[ListservMessage]],
        fields: str = "total",
    ) -> "ListservList":
        """"""
        if not messages:
            msgs = messages
        elif isinstance(messages[0], str):
            msgs = []
            for idx, url in enumerate(messages):
                msgs.append(
                    ListservMessage.from_url(
                        list_name=name,
                        url=url,
                        fields=fields,
                    )
                )
        else:
            msgs = messages
        return cls(name, url, msgs)

    @classmethod
    def get_messages(
        cls,
        name: str,
        url: str,
        select: dict,
    ) -> List[ListservMessage]:
        """
        Generator that yields all messages within a certain period
        (e.g. January 2021, Week 5).

        Args:
            datatype: [mbox, dataframe]

        Returns:
        """
        msgs = []
        # run through periods
        for period_url in ListservList.get_period_urls(url, select):
            # run through messages within period
            for msg_url in ListservList.get_messages_urls(name, period_url):
                msgs.append(
                    ListservMessage.from_url(name, msg_url, select["fields"])
                )
                # wait between loading messages, for politeness
                time.sleep(1)
        return msgs

    @classmethod
    def get_period_urls(cls, url: str, select: dict) -> List[str]:
        """
        all messages within a certain period
        (e.g. January 2021, Week 5).
        """
        url_root = ("/").join(url.split("/")[:-2])
        soup = get_website_content(url)
        # create dictionary with key indicating period and values the url
        periods = [list_tag.find("a").text for list_tag in soup.find_all("li")]
        urls_of_periods = [
            urllib.parse.urljoin(url_root, list_tag.find("a").get("href"))
            for list_tag in soup.find_all("li")
        ]

        for key, value in select.items():
            if key == "years":
                cond = lambda x: int(re.findall(r"\d{4}", x)[0])
            elif key == "months":
                cond = lambda x: x.split(" ")[0]
            elif key == "weeks":
                cond = lambda x: int(x.split(" ")[-1])
            else:
                continue

            periodquants = [cond(period) for period in periods]

            indices = ListservList.get_index_of_elements_in_selection(
                periodquants,
                urls_of_periods,
                value,
            )

            periods = [periods[idx] for idx in indices]
            urls_of_periods = [urls_of_periods[idx] for idx in indices]
        return urls_of_periods

    @staticmethod
    def get_index_of_elements_in_selection(
        times: list,
        urls: List[str],
        filtr: Union[tuple, list, int, str],
    ) -> List[int]:
        """
        Filter out messages that where in a specific period. Period here is a set
        containing units of years, months, and weeks which can have the following
        example elements:
            - years: (1992, 2010), [2000, 2008], 2021
            - months: ["January", "July"], "November"
            - weeks: (1, 4), [1, 5], 2

        Args:
        Returns:
        """
        if isinstance(filtr, tuple):
            # filter year or week in range
            cond = lambda x: (np.min(filtr) <= x <= np.max(filtr))
        if isinstance(filtr, list):
            # filter in year, week, or month in list
            cond = lambda x: x in filtr
        if isinstance(filtr, int):
            # filter specific year or week
            cond = lambda x: x == filtr
        if isinstance(filtr, str):
            # filter specific month
            cond = lambda x: x == filtr
        return [idx for idx, time in enumerate(times) if cond(time)]

    @classmethod
    def get_messages_urls(cls, name: str, url: str) -> List[str]:
        """
        Args:
            url: URL to period.
            fields: [total, header]

        Returns:
            List to message URLs.
        """
        url_root = ("/").join(url.split("/")[:-2])
        soup = get_website_content(url)
        a_tags = soup.select(f'a[href*="A2="][href*="{name}"]')
        if a_tags:
            a_tags = [
                urllib.parse.urljoin(url_root, url.get("href"))
                for url in a_tags
            ]
        return a_tags

    def to_dict(self) -> Dict[str, List[str]]:
        """
        Place all message into a dictionary of the form:
            dic = {
                "Subject": [messages[0], ... , messages[n]],
                .
                .
                .
                "ContentType": [messages[0], ... , messages[n]]
            }
        """
        # initialize dictionary
        dic = {}
        for key in list(self.messages[0].to_dict().keys()):
            dic[key] = []
        # run through messages
        for msg in self.messages:
            # run through message attributes
            for key, value in msg.to_dict().items():
                dic[key].append(value)
        return dic

    def to_pandas_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame.from_dict(self.to_dict())

    # def to_mbox(
    #    self,
    #    dir_out: str,
    #    fields: Union[List[str], str] = "total",
    # ):
    #    """
    #    Save Archive content to .mbox files

    #    Args:

    #    Returns:
    #    """
    #    for period in self.yield_period(fields):
    #        file_name = (
    #            period["name"].replace(" ", "_").replace(",", "").strip()
    #        )
    #        file_path = dir_out + file_name + ".mbox"
    #        mbox = mailbox.mbox(file_path)
    #        mbox.lock()
    #        try:
    #            [mbox.add(message) for message in period["messages"]]
    #            mbox.flush()
    #        finally:
    #            mbox.unlock()


class ListservArchive(object):
    """
    This class handles a public mailing list archive that uses the
    LISTSERV 16.5 format.
    An archive is a list of ListservList elements.

    Parameters
    ----------
    name
        The of whom the archive is (e.g. 3GPP, IEEE, ...)
    url
        The URL where the archive lives
    lists
        A list containing the mailing lists as `ListservList` types

    Methods
    -------
    from_url
    from_mailing_lists
    get_lists
    get_sections
    to_dict
    to_pandas_dataframe
    to_mbox

    Example
    -------
    arch = ListservArchive.from_url(
        "3GPP",
        "https://list.etsi.org/scripts/wa.exe?",
        "https://list.etsi.org/scripts/wa.exe?HOME",
        select={
            "years": (2020, 2021),
            "months": "January",
            "weeks": [1,5],
            "fields": "header",
        },
    )
    """

    def __init__(self, name: str, url: str, lists: List[ListservList]):
        self.name = name
        self.url = url
        self.lists = lists

    def __len__(self):
        return len(self.lists)

    def __iter__(self):
        return iter(self.lists)

    def __getitem__(self, index):
        return self.lists[index]

    @classmethod
    def from_url(
        cls,
        name: str,
        url_root: str,
        url_home: str,
        select: dict,
    ) -> "ListservArchive":
        """
        Create ListservArchive from a given URL.

        Args:
            name:
            url_root:
            url_home:
            select:
        """
        lists = cls.get_lists(url_root, url_home, select)
        return cls.from_mailing_lists(name, url_root, lists, select)

    @classmethod
    def from_mailing_lists(
        cls,
        name: str,
        url_root: str,
        url_mailing_lists: Union[List[str], List[ListservList]],
        select: dict,
    ) -> "ListservArchive":
        """
        Create ListservArchive from a given list of 'ListservList'.

        Args:
            name:
            url_root:
            url_mailing_lists:

        """
        if isinstance(url_mailing_lists[0], str):
            lists = []
            for idx, url in enumerate(url_mailing_lists):
                lists.append(
                    ListservList.from_url(name=idx, url=url, select=select)
                )
        else:
            lists = url_mailing_lists
        return cls(name, url_root, lists)

    @staticmethod
    def get_lists(
        url_root: str,
        url_home: str,
        select: dict,
    ) -> List[ListservList]:
        """
        Created dictionary of all lists in the archive.

        Args:

        Returns:
            archive_dict: the keys are the names of the lists and the value their url
        """
        archive = []
        # run through archive sections
        for url in list(
            ListservArchive.get_sections(url_root, url_home).keys()
        )[:1]:
            soup = get_website_content(url)
            a_tags_in_section = soup.select(
                'a[href*="A0="][onmouseover*="showDesc"][onmouseout*="hideDesc"]',
            )

            # run through archive lists in section
            for a_tag in a_tags_in_section:
                value = urllib.parse.urljoin(url_root, a_tag.get("href"))
                key = value.split("A0=")[-1]
                mlist = ListservList.from_url(
                    name=key, url=value, select=select
                )
                if len(mlist) != 0:
                    archive.append(mlist)
        return archive

    def get_sections(url_root: str, url_home: str) -> int:
        """
        Get different sections of archive. On the website they look like:
        [3GPP] [3GPP–AT1] [AT2–CONS] [CONS–EHEA] [EHEA–ERM_] ...

        Returns:
            If sections exist, it returns their urls and names. Otherwise it returns
            the url_home.
        """
        soup = get_website_content(url_home)
        sections = soup.select(
            'a[href*="INDEX="][href*="p="]',
        )
        archive_sections_dict = {}
        if sections:
            for sec in sections:
                key = urllib.parse.urljoin(url_root, sec.get("href"))
                value = sec.text
                if value in ["Next", "Previous"]:
                    continue
                archive_sections_dict[key] = value
            # TODO check that p=1 is included
        else:
            archive_sections_dict[url_home] = "Home"
        return archive_sections_dict

    def to_dict(self) -> Dict[str, List[str]]:
        """
        Place all message in all lists into a dictionary of the form:
            dic = {
                "Subject": [messages[0], ... , messages[n]],
                .
                .
                .
                "ListName": [messages[0], ... , messages[n]]
                "ListURL": [messages[0], ... , messages[n]]
            }
        """
        # initialize dictionary
        dic = {}
        for key in list(self.lists[0].messages[0].to_dict().keys()):
            dic[key] = []
        dic["ListName"] = []
        dic["ListURL"] = []
        # run through lists
        for mlist in self.lists:
            # run through messages
            for msg in mlist.messages:
                # run through message attributes
                for key, value in msg.to_dict().items():
                    dic[key].append(value)
                dic["ListName"].append(mlist.name)
                dic["ListURL"].append(mlist.url)
        return dic

    def to_pandas_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame.from_dict(self.to_dict())

    def to_mbox(self, dir_out: str):
        """
        Save Archive content to .mbox files
        """
        for llist in self.lists[1:2]:
            llist.to_mbox(dir_out)


def get_website_content(
    url: str,
) -> BeautifulSoup:
    """"""
    resp = urllib.request.urlopen(url)
    assert resp.getcode() == 200
    # TODO: include option to change BeautifulSoup args
    return BeautifulSoup(resp.read(), features="lxml")


if __name__ == "__main__":
    arch = ListservArchive.from_url(
        "3GPP",
        url_root="https://list.etsi.org/scripts/wa.exe?",
        url_home="https://list.etsi.org/scripts/wa.exe?HOME",
        select={
            "years": 2021,
            "months": "January",
            "weeks": 1,
            "fields": "total",
        },
    )
    print(f"Lenght of arch = {len(arch)}")
    print(arch.to_dict()["Subject"])
    mlist = ListservList.from_url(
        "3GPP_TSG_CT_WG6",
        url="https://list.etsi.org/scripts/wa.exe?A0=3GPP_TSG_CT_WG6",
        # select={"years": (2021, 2021)},
        select={
            "years": 2021,
            "months": "January",
            "weeks": 1,
            "fields": "header",
        },
    )
    print(f"Lenght of mlist = {len(mlist)}")
    print(mlist.messages[0].Subject)
    print(mlist.messages[1].Subject)
    print(mlist.messages[2].Subject)
    # test = arch.to_mbox("/home/christovis/02_AGE/datactive/")
