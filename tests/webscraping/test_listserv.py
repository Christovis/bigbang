import os
import tempfile
from pathlib import Path
from unittest import mock

import pytest
import yaml

import bigbang
from bigbang import listserv
from bigbang.listserv import ListservArchive, ListservList, ListservMessage
from config.config import CONFIG

dir_temp = tempfile.gettempdir()
project_directory = str(Path(os.path.abspath(__file__)).parent.parent.parent)
file_temp_mbox = dir_temp + "/listserv.mbox"
file_auth = project_directory + "/config/authentication.yaml"
auth_key_mock = {"username": "bla", "password": "bla"}

urls = {
    "3GPP": {
        "archive": "https://list.etsi.org/scripts/wa.exe?",
        "list": "https://list.etsi.org/scripts/wa.exe?A0=3GPP_TSG_CT_WG6",
        "message": "https://list.etsi.org/scripts/wa.exe?A2=ind2101A&L=3GPP_TSG_CT_WG6&O=D&P=1870",
    },
    "IEEE": {
        "archive": "https://listserv.ieee.org/cgi-bin/wa?",
        "list": "https://listserv.ieee.org/cgi-bin/wa?A0=TORONTO-YP",
        "message": "https://listserv.ieee.org/cgi-bin/wa?A2=ind14&L=TORONTO-YP&P=62",
    },
}


class Test3GPPListservMessage:
    @pytest.mark.skipif(
        not os.path.isfile(file_auth),
        reason="Key to log into LISTSERV could not be found",
    )
    def test__from_url_with_login(self):
        with open(file_auth, "r") as stream:
            auth_key = yaml.safe_load(stream)
        msg = ListservMessage.from_url(
            list_name="3GPP_TSG_CT_WG6",
            url=urls["3GPP"]["message"],
            fields="total",
            login=auth_key,
        )
        assert msg.fromaddr == "Kimmo.Kymalainen@ETSI.ORG"
        assert msg.toaddr == "Kimmo.Kymalainen@ETSI.ORG"

    @pytest.fixture(name="msg", scope="module")
    def test__from_url_without_login(self):
        msg = ListservMessage.from_url(
            list_name="3GPP_TSG_CT_WG6",
            url=urls["3GPP"]["message"],
            fields="total",
            login=auth_key_mock,
        )
        assert msg.body.split(",")[0] == "Dear 3GPP CT people"
        assert msg.subject == "Happy New Year 2021"
        assert msg.fromname == "Kimmo Kymalainen"
        assert msg.fromaddr == "[log in to unmask]"
        assert msg.toname == "Kimmo Kymalainen"
        assert msg.toaddr == "[log in to unmask]"
        assert msg.date == "Tue Jan  5 12:15:30 2021"
        assert msg.contenttype == "multipart/related"
        return msg

    def test__only_header_from_url(self):
        msg = ListservMessage.from_url(
            list_name="3GPP_TSG_CT_WG6",
            url=urls["3GPP"]["message"],
            fields="header",
            login=auth_key_mock,
        )
        assert msg.body is None

    def test__only_body_from_url(self):
        msg = ListservMessage.from_url(
            list_name="3GPP_TSG_CT_WG6",
            url=urls["3GPP"]["message"],
            fields="body",
            login=auth_key_mock,
        )
        assert msg.subject is None

    def test__to_dict(self, msg):
        dic = msg.to_dict()
        assert len(list(dic.keys())) == 8

    def test__to_mbox(self, msg):
        msg.to_mbox(file_temp_mbox)
        f = open(file_temp_mbox, "r")
        lines = f.readlines()
        assert len(lines) == 29
        assert (
            lines[1] == "From b'[log in to unmask]' Tue Jan  5 12:15:30 2021\n"
        )
        f.close()
        Path(file_temp_mbox).unlink()


class TestIEEEListservMessage:
    def test__from_IEEE_url_without_login(self):
        msg = ListservMessage.from_url(
            list_name="TORONTO-YP",
            url=urls["IEEE"]["message"],
            fields="total",
            login=auth_key_mock,
        )
        print(msg.toaddr, msg.fromaddr)
        assert (
            msg.body.split(",")[0] == "This is testing the ListServ. Currently"
        )
        assert msg.subject == "Test"
        assert msg.fromname == "Robert Vice"
        assert msg.fromaddr == "[log in to unmask]"
        assert msg.toname == "Robert Vice"
        assert msg.toaddr == "[log in to unmask]"
        assert msg.date == "Thu Dec 11 10:44:35 2014"
        assert msg.contenttype == "text/plain"


class Test3GPPListservList:
    @pytest.mark.skipif(
        not os.path.isfile(file_auth),
        reason="Key to log into LISTSERV could not be found",
    )
    def test__from_url_with_login(self):
        with open(file_auth, "r") as stream:
            auth_key = yaml.safe_load(stream)
        mlist = ListservList.from_url(
            name="3GPP_TSG_CT_WG6",
            url=urls["3GPP"]["list"],
            select={
                "years": 2021,
                "months": "January",
                "weeks": 1,
                "fields": "header",
            },
            login=auth_key,
        )
        assert mlist.messages[0].fromaddr == "Kimmo.Kymalainen@ETSI.ORG"
        assert mlist.messages[0].toaddr == "Kimmo.Kymalainen@ETSI.ORG"

    @pytest.fixture(name="mlist", scope="module")
    def test__from_url_without_login(self):
        mlist = ListservList.from_url(
            name="3GPP_TSG_CT_WG6",
            url=urls["3GPP"]["list"],
            select={
                "years": 2021,
                "months": "January",
                "weeks": 1,
                "fields": "header",
            },
            login=auth_key_mock,
        )
        assert mlist.name == "3GPP_TSG_CT_WG6"
        assert mlist.source == urls["3GPP"]["list"]
        assert len(mlist) == 3
        assert mlist.messages[0].subject == "Happy New Year 2021"
        return mlist

    def test__to_dict(self, mlist):
        dic = mlist.to_dict()
        assert len(list(dic.keys())) == 8
        assert len(dic[list(dic.keys())[0]]) == 3

    def test__to_pandas_dataframe(self, mlist):
        df = mlist.to_pandas_dataframe()
        assert len(df.columns.values) == 8
        assert len(df.index.values) == 3

    def test__to_mbox(self, mlist):
        mlist.to_mbox(dir_temp, filename=mlist.name)
        file_temp_mbox = f"{dir_temp}/{mlist.name}.mbox"
        f = open(file_temp_mbox, "r")
        lines = f.readlines()
        assert len(lines) == 30
        assert (
            lines[21]
            == "From b'[log in to unmask]' Tue Jan  5 09:28:25 2021\n"
        )
        f.close()
        Path(file_temp_mbox).unlink()


class TestIEEEListservList:
    def test__list_from_IEEE_url_with_login(self):
        mlist = ListservList.from_url(
            name="TORONTO-YP",
            url=urls["IEEE"]["list"],
            login=auth_key_mock,
        )
        assert len(mlist) == 1
        assert mlist.messages[0].subject == "Test"
        assert mlist.messages[0].fromname == "Robert Vice"


class Test3GPPListservArchive:
    @pytest.mark.skipif(
        not os.path.isfile(file_auth),
        reason="Key to log into LISTSERV could not be found",
    )
    def test__from_url_with_login(self):
        with open(file_auth, "r") as stream:
            auth_key = yaml.safe_load(stream)
        arch = ListservArchive.from_url(
            name="3GPP",
            url_root=urls["3GPP"]["archive"],
            url_home=urls["3GPP"]["archive"] + "HOME",
            select={
                "years": 2021,
                "months": "January",
                "weeks": 1,
                "fields": "header",
            },
            login=auth_key,
            instant_dump=False,
        )
        assert (
            arch.lists[0].messages[0].fromaddr == "Kimmo.Kymalainen@ETSI.ORG"
        )
        assert arch.lists[0].messages[0].toaddr == "Kimmo.Kymalainen@ETSI.ORG"

    @pytest.fixture(name="arch", scope="session")
    def test__from_url_wihout_login(self):
        arch = ListservArchive.from_url(
            name="3GPP",
            url_root=urls["3GPP"]["archive"],
            url_home=urls["3GPP"]["archive"] + "HOME",
            select={
                "years": 2021,
                "months": "January",
                "weeks": 1,
                "fields": "header",
            },
            login=auth_key_mock,
            instant_dump=False,
        )
        assert arch.name == "3GPP"
        assert arch.url == urls["3GPP"]["archive"]
        assert len(arch) == 4
        assert len(arch.lists[0]) == 3
        assert arch.lists[0].messages[0].subject == "Happy New Year 2021"
        return arch

    def test__to_dict(self, arch):
        dic = arch.to_dict()
        assert len(list(dic.keys())) == 9
        assert len(dic[list(dic.keys())[0]]) == 40

    def test__to_pandas_dataframe(self, arch):
        df = arch.to_pandas_dataframe()
        assert len(df.columns.values) == 9
        assert len(df.index.values) == 40

    def test__to_mbox(self, arch):
        arch.to_mbox(dir_temp)
        file_dic = {
            f"{dir_temp}/3GPP_TSG_CT_WG6.mbox": 30,
            f"{dir_temp}/3GPP_TSG_RAN_WG3.mbox": 40,
            f"{dir_temp}/3GPP_TSG_RAN.mbox": 30,
            f"{dir_temp}/3GPP_TSG_RAN_WG4.mbox": 300,
        }
        for filepath, line_nr in file_dic.items():
            assert Path(filepath).is_file()
            f = open(filepath, "r")
            lines = f.readlines()
            assert line_nr == len(lines)
            f.close()
            Path(filepath).unlink()


@mock.patch("bigbang.listserv.ask_for_input", return_value="check")
def test__get_login_from_terminal(input):
    """ test if login keys will be documented """
    file_auth = dir_temp + "/authentication.yaml"
    _, _ = listserv.get_login_from_terminal(
        username=None, password=None, file_auth=file_auth
    )
    f = open(file_auth, "r")
    lines = f.readlines()
    assert lines[0].strip("\n") == "username: 'check'"
    assert lines[1].strip("\n") == "password: 'check'"
    os.remove(file_auth)
