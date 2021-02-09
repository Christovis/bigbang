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
        self.Subject = Subject
        self.Body = body
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
        fields: Optional[str] = None,
    ) -> "ListservMessage":
        """
        Args:
        """
        # TODO implement field selection, e.g. return only header, body, etc.
        soup = get_website_content(url)
        header = ListservMessage.get_header(soup)
        body = ListservMessage.get_body(list_name, url, soup)
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

        header["FromName"] = listserv.get_name(header["From"])
        header["FromAddr"] = listserv.get_from(header["From"])
        header["ToName"] = listserv.get_name(header["Reply-To"])
        header["ToAddr"] = listserv.get_from(header["Reply-To"])
        header["Date"] = listserv.get_date(header["Date"])
        header["ContentType"] = header["Content-Type"]
        del header["From"], header["Reply-To"], header["Content-Type"]
        return header


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

    Example
    -------
    mlist = ListservList.from_url(
        "3GPP_TSG_CT_WG6",
        url="https://list.etsi.org/scripts/wa.exe?A0=3GPP_TSG_CT_WG6",
        select={"years": (2020, 2021)},
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

    def __getitem__(self, index):
        return self.messages[index]

    @classmethod
    def from_url(
        cls,
        name: str,
        url: str,
        select: Dict[str, tuple],
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
        msgs = cls.get_messages(name, url, select["fields"], select["years"])
        return cls(name, url, msgs)

    @classmethod
    def get_messages(
        cls,
        name: str,
        url: str,
        fields: str,
        filter_yrs: tuple,
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
        for period_url in ListservList.get_period_urls(url, filter_yrs):
            # run through messages within period
            for msg_url in ListservList.get_messages_urls(name, period_url):
                msgs.append(ListservMessage.from_url(name, msg_url, fields))
                # wait between loading messages, for politeness
                time.sleep(1)
        return msgs

    @classmethod
    def get_period_urls(cls, url: str, filter_yrs: tuple) -> List[str]:
        """
        all messages within a certain period
        (e.g. January 2021, Week 5).
        """
        url_root = ("/").join(url.split("/")[:-2])
        soup = get_website_content(url)
        # get links to all messages within this mailing list
        links = {
            list_tag.find("a").text: urllib.parse.urljoin(
                url_root, list_tag.find("a").get("href")
            )
            for list_tag in soup.find_all("li")
        }
        if filter_yrs:
            # select messages send in time range defined by the filter
            links = [
                links[period]
                for period in list(links.keys())
                if (
                    np.min(filter_yrs)
                    <= int(re.findall(r"\d{4}", period)[0])
                    <= np.max(filter_yrs)
                )
            ]
        return links

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

    def to_dataframe(
        self,
        fields: Union[List[str], str] = "total",
    ):
        """
        Scrape mailing list and fill into a pandas.DataFrame.

        Args:
        Returns:
        """
        pass

    def to_mbox(
        self,
        dir_out: str = "/home/christovis/02_AGE/datactive/bigbang/archives/bigbang/archives/",
        fields: Union[List[str], str] = "total",
    ):
        """
        Save Archive content to .mbox files

        Args:

        Returns:
        """
        for period in self.yield_period(fields):
            file_name = (
                period["name"].replace(" ", "_").replace(",", "").strip()
            )
            file_path = dir_out + file_name + ".mbox"
            mbox = mailbox.mbox(file_path)
            mbox.lock()
            try:
                [mbox.add(message) for message in period["messages"]]
                mbox.flush()
            finally:
                mbox.unlock()


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
    to_mbox

    Example
    -------
    arch = ListservArchive.from_url(
        "3GPP",
        "https://list.etsi.org/scripts/wa.exe?",
        "https://list.etsi.org/scripts/wa.exe?HOME",
        {"years": (2019, 2021)},
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
        select: Dict[str, tuple],
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
        select: Dict[str, tuple],
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
                lists.append(ListservList(name=idx, url=url, select=select))
        else:
            lists = url_mailing_lists
        return cls(name, url_root, lists)

    @staticmethod
    def get_lists(
        url_root: str,
        url_home: str,
        select: Dict[str, tuple],
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
                archive.append(
                    ListservList.from_url(name=key, url=value, select=select)
                )

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
    # arch = ListservArchive.from_url(
    #    "3GPP",
    #    url_root="https://list.etsi.org/scripts/wa.exe?",
    #    url_home="https://list.etsi.org/scripts/wa.exe?HOME",
    #    select={"years": (2020, 2021)},
    # )
    mlist = ListservList.from_url(
        "3GPP_TSG_CT_WG6",
        url="https://list.etsi.org/scripts/wa.exe?A0=3GPP_TSG_CT_WG6",
        select={"years": (2021, 2021)},
    )
    # test = arch.to_mbox("/home/christovis/02_AGE/datactive/")
