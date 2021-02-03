import time
import numpy as np
from typing import Dict, List, Optional, Union
import pprint

import codecs
import datetime
import fnmatch
import gzip
import logging
import mailbox
import os
import re
import subprocess
import urllib.error
import urllib.parse
import urllib.request

import email
import email.parser
from email.mime.text import MIMEText
from email.message import Message
from email.header import Header

import warnings
from pprint import pprint as pp

from bs4 import BeautifulSoup

import pandas as pd
import yaml
from validator_collection import checkers

import bigbang.listserv as listserv

pp = pprint.PrettyPrinter(indent=4)


class ListservArchiveWarning(BaseException):
    """Base class for Archive class specific exceptions"""
    pass



class ListservList:

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
        print("name = ", self.name)
        print("url = ", self.url)
        for period in self.yield_period(fields):
            file_name = period["name"].replace(" ", "_").replace(",", "").strip()
            file_path = dir_out + file_name + ".mbox"
            print("^^^^^^^ file_path ^^^^^^^", file_path)
            mbox = mailbox.mbox(file_path)
            mbox.lock()
            try:
                [mbox.add(message) for message in period["messages"]]
                mbox.flush()
            finally:
                mbox.unlock()

            break

    def yield_period(
        self,
        fields: Union[List[str], str] = "total",
        filter_yr: tuple = (2020, 2021),
    ):
        """
        Generator that yields all messages within period
        """
        soup = get_website_content(self.url)

        links = {
            list_tag.find("a").text: urllib.parse.urljoin(
                self.url_root, list_tag.find("a").get('href')
            )
            for list_tag in soup.find_all("li")
        }

        if filter_yr:
            links = {
                period: links[period]
                for period in list(links.keys())
                if (
                    np.min(filter_yr) <=
                    int(re.findall("\d{4}", period)[0]) <=
                    np.max(filter_yr)
                )
            }

        # run through periods
        for key, link in links.items():
            print("link --------", link)

            msg_in_period = []
            # run through messages within period
            for msg in self.yield_message(link, fields):
                msg_in_period.append(msg)
                time.sleep(1)  # wait between loading messages, for politeness

            yield {"name": key, "messages": msg_in_period}

    def yield_message(
        self,
        url: str,
        fields: str = "total",
    ):
        """
        Args:
            fields: [total, header]
        """
        soup = get_website_content(url)
        a_tags = soup.select(f'a[href*="A2="][href*="{self.name}"]',)
        if a_tags:
            for a_tag in a_tags:
                value = urllib.parse.urljoin(self.url_root, a_tag.get('href'))
                soup = get_website_content(value)
                if fields == "total":
                    msg = self.create_message_body(soup)
                else:
                    msg = MIMEText(" ", 'plain', 'utf-8')
                mbox_msg = self.add_message_header(soup, msg)
                yield mbox_msg
        else:
            yield None

    def create_message_body(self, soup: BeautifulSoup) -> MIMEText:
        a_tags = soup.select(f'a[href*="A3="][href*="{self.name}"]')
        href_plain_text = [
            tag.get('href')
            for tag in a_tags
            if "Fplain" in tag.get('href')
        ][0]
        body_soup = get_website_content(
            urllib.parse.urljoin(self.url_root, href_plain_text)
        )
        body = MIMEText(body_soup.find("pre").text, 'plain', 'utf-8')
        return body

    def add_message_header(
        self, soup: BeautifulSoup, msg: MIMEText,
    ) -> mailbox.mboxMessage:
        text = soup.find(
            "b", text=re.compile(r'^\bSubject\b'),
        ).parent.parent.parent.parent.text

        header_fields = {}
        for field in text.split("Parts/Attachments:")[0].splitlines():
            if len(field) == 0: continue
            field_name = field.split(":")[0].strip()
            field_body = field.replace(field_name+":", "").strip()
            header_fields[field_name] = field_body

        print("Subject --------", header_fields["Subject"])
        msg["Subject"] = header_fields["Subject"]
        msg["From"] = email.utils.formataddr((
            listserv.get_name(header_fields["From"]),
            listserv.get_from(header_fields["From"]),
        ))
        msg["To"] = email.utils.formataddr((
            listserv.get_name(header_fields["Reply-To"]),
            listserv.get_from(header_fields["Reply-To"]),
        ))
        msg["Date"] = listserv.get_date(header_fields["Date"])
        mbox_msg = mailbox.mboxMessage(msg)
        mbox_msg.set_from(
            listserv.get_from(header_fields["From"]),
            email.utils.parsedate(listserv.get_date(header_fields["Date"])),
        )
        return mbox_msg

    def create_file_structure(self):
        pass


class ListservArchive(object):
    """
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
        url_root: str = "https://list.etsi.org/scripts/wa.exe?",
        url_home: str = "https://list.etsi.org/scripts/wa.exe?HOME",
        filters: dict = {"year": (2019, 2021)},
        dir_out: str = "./",
    ):
        """
        An archive is a list of ListservList elements

        Args:
        Returns:
        """
        lists = cls.get_lists(url_root, url_home)
        #for archlist in generate_list_content():
        #activity_to_archive_lists(url_root, archive_lists_dict)
        #pp.pprint(archive_lists_dict)
        return cls("3GPP", url_root, lists)

    def get_sections(url_root: str, url_home: str) -> int:
        """
        Get different sections of archive. On the website they look like:
        [3GPP] [3GPP–AT1] [AT2–CONS] [CONS–EHEA] [EHEA–ERM_] ...

        Returns:
            If sections exist, it returns their urls and names. Otherwise it returns
            the url_home.
        """
        soup = get_website_content(url_home)
        sections = soup.select('a[href*="INDEX="][href*="p="]',)

        archive_sections_dict = {}
        if sections:
            for sec in sections:
                key = urllib.parse.urljoin(url_root, sec.get('href'))
                value = sec.text
                if value in ["Next", "Previous"]: continue
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
        for url in list(ListservArchive.get_sections(url_root, url_home).keys())[:1]:
            soup = get_website_content(url)
            a_tags_in_section = soup.select(
                'a[href*="A0="][onmouseover*="showDesc"][onmouseout*="hideDesc"]',
            )

            # run through archive lists in section
            for a_tag in a_tags_in_section:
                value = urllib.parse.urljoin(url_root, a_tag.get('href'))
                key = value.split("A0=")[-1]
                archive.append(
                    ListservList(name=key, url=value)
                )

        return archive

    def to_mbox(self, dir_out: str):
        """
        Save Archive content to .mbox files
        """
        for llist in self.lists[1:2]:
            llist.to_mbox(dir_out)


def get_website_content(url:str, ) -> BeautifulSoup:
    """
    """
    resp = urllib.request.urlopen(url)
    assert resp.getcode() == 200
    # TODO: include option to change BeautifulSoup args
    return BeautifulSoup(resp.read(), features="lxml")


if __name__ == "__main__":
    arch = ListservArchive.from_url()
    test = arch.to_mbox("/home/christovis/02_AGE/datactive/")
