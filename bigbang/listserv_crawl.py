import codecs
import datetime
import email
import email.parser
import fnmatch
import gzip
import logging
import mailbox
import os
import pprint
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
from pprint import pprint as pp
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
    """

    def __init__(self, name: str, url: str):
        self.name = name
        self.url = url
        self.url_root = ("/").join(url.split("/")[:-2])
        self.periods = {}
        self.messages = {}

    def __len__(self) -> int:
        return len(self.messages)

    def __iter__(self):
        return iter(self.messages)

    def __getitem__(self, index):
        return self.messages[index]

    # @classmethod
    # def from_url(
    #     cls,
    #     name: str = "3GPP",
    #     url: str = "https://list.etsi.org/scripts/wa.exe?",
    #     fields: Union[List[str], str] = "total",
    #     filter_perios: tuple = (2020, 2021),
    #     datatype: str = "mbox",
    # ) -> "ListservList":
    #     """
    #     Args:
    #     """
    #     for period in self.yield_period(fields, filter_perios, datatype):
    #     return cls(name, url, ...)

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

    def yield_period(
        self,
        fields: Union[List[str], str] = "total",
        filter_yr: tuple = (2020, 2021),
        datatype: str = "mbox",
    ):
        """
        Generator that yields all messages within a certain period
        (e.g. January 2021, Week 5).

        Args:
            datatype: [mbox, dataframe]

        Returns:
        """
        soup = get_website_content(self.url)

        links = {
            list_tag.find("a").text: urllib.parse.urljoin(
                self.url_root, list_tag.find("a").get("href")
            )
            for list_tag in soup.find_all("li")
        }

        if filter_yr:
            # filter out messages send in time range defined by the filter
            links = {
                period: links[period]
                for period in list(links.keys())
                if (
                    np.min(filter_yr)
                    <= int(re.findall(r"\d{4}", period)[0])
                    <= np.max(filter_yr)
                )
            }

        # run through periods
        for key, link in links.items():

            if datatype == "mbox":
                msg_in_period = []
                # run through messages within period
                for msg in self.yield_message(link, fields):
                    msg_in_period.append(msg)
                    time.sleep(
                        1
                    )  # wait between loading messages, for politeness

            elif datatype == "dataframe":
                pass

            yield {"name": key, "messages": msg_in_period}

    def yield_message(
        self,
        url: str,
        fields: str = "total",
        datatype: str = "mbox",
    ):
        """
        Args:
            fields: [total, header]
        """
        soup = get_website_content(url)
        a_tags = soup.select(
            f'a[href*="A2="][href*="{self.name}"]',
        )
        if a_tags:
            for a_tag in a_tags:
                value = urllib.parse.urljoin(self.url_root, a_tag.get("href"))
                soup = get_website_content(value)
                if fields == "total":
                    msg = self.get_message_body(soup)
                else:
                    msg = MIMEText(" ", "plain", "utf-8")
                msg = self.get_message_header(soup, msg)
                msg["Body"] = self.get_message_body(soup)
                yield msg
        else:
            yield None

    def get_message_body(self, soup: BeautifulSoup) -> str:  # MIMEText:
        a_tags = soup.select(f'a[href*="A3="][href*="{self.name}"]')
        href_plain_text = [
            tag.get("href") for tag in a_tags if "Fplain" in tag.get("href")
        ][0]
        body_soup = get_website_content(
            urllib.parse.urljoin(self.url_root, href_plain_text)
        )
        # body = MIMEText(body_soup.find("pre").text, 'plain', 'utf-8')
        return body_soup.find("pre").text

    def add_message_header(
        self,
        soup: BeautifulSoup,
        msg: MIMEText,
    ) -> dict:  # mailbox.mboxMessage:
        text = soup.find(
            "b",
            text=re.compile(r"^\bSubject\b"),
        ).parent.parent.parent.parent.text

        header_fields = {}
        for field in text.split("Parts/Attachments:")[0].splitlines():
            if len(field) == 0:
                continue
            field_name = field.split(":")[0].strip()
            field_body = field.replace(field_name + ":", "").strip()
            header_fields[field_name] = field_body

        header_fields["FromName"] = listserv.get_name(header_fields["From"])
        header_fields["FromAddr"] = listserv.get_from(header_fields["From"])
        header_fields["ToName"] = listserv.get_name(header_fields["Reply-To"])
        header_fields["ToAddr"] = listserv.get_from(header_fields["Reply-To"])
        header_fields["Date"] = listserv.get_date(header_fields["Date"])

        # msg["Subject"] = header_fields["Subject"]
        # msg["From"] = email.utils.formataddr((
        #    listserv.get_name(header_fields["From"]),
        #    listserv.get_from(header_fields["From"]),
        # ))
        # msg["To"] = email.utils.formataddr((
        #    listserv.get_name(header_fields["Reply-To"]),
        #    listserv.get_from(header_fields["Reply-To"]),
        # ))
        # msg["Date"] = listserv.get_date(header_fields["Date"])
        # mbox_msg = mailbox.mboxMessage(msg)
        # mbox_msg.set_from(
        #    listserv.get_from(header_fields["From"]),
        #    email.utils.parsedate(listserv.get_date(header_fields["Date"])),
        # )
        return header_fields


class ListservArchive(object):
    """
    This class handles a public mailing list archive that uses the
    LISTSERV 16.5 format.

    Parameters
    ----------
    name
        The of whom the archive is (e.g. 3GPP, IEEE, ...)
    url
        The URL where the archive lives
    lists
        A list containing the mailing lists as `ListservList` types
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
        name: str = "3GPP",
        url_root: str = "https://list.etsi.org/scripts/wa.exe?",
        url_home: str = "https://list.etsi.org/scripts/wa.exe?HOME",
        filter_yr: Dict[str, tuple] = {"year": (2019, 2021)},
    ):
        """
        An archive is a list of ListservList elements

        Args:
        Returns:
        """
        lists = cls.get_lists(url_root, url_home)
        return cls.from_mailing_lists(name, url_root, lists)

    @classmethod
    def from_mailing_lists(
        cls,
        name: str,
        url_root: str,
        url_mailing_lists: Union[List[str], List[ListservList]],
    ):
        """
        An archive is a list of ListservList elements

        Args:
        Returns:
        """
        if isinstance(url_mailing_lists[0], str):
            lists = []
            for idx, url in enumerate(url_mailing_lists):
                lists.append(ListservList(name=idx, url=url))
        else:
            lists = url_mailing_lists
        return cls(name, url_root, lists)

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

    @staticmethod
    def get_lists(url_root: str, url_home: str) -> List[ListservList]:
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
                archive.append(ListservList(name=key, url=value))

        return archive

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
        url_root="https://list.etsi.org/scripts/wa.exe?",
        url_home="https://list.etsi.org/scripts/wa.exe?HOME",
        filter_yr={"year": (2019, 2021)},
    )
    test = arch.to_mbox("/home/christovis/02_AGE/datactive/")
