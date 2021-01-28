import datetime
import logging
import mailbox
from typing import List, Optional, Union

import numpy as np
import pandas as pd
import pytz

import bigbang.process as process
from bigbang.thread import Node, Thread
from config.config import CONFIG

from . import listserv, mailman, utils


class ArchiveWarning(BaseException):
    """Base class for Archive class specific exceptions"""

    pass


def load(path):
    data = pd.read_csv(path)
    return Archive(data)


class Archive(object):
    """A representation of a mailing list archive."""

    data = None
    activity = None
    threads = None
    entities = None

    def __init__(
        self,
        df: pd.DataFrame,
        mbox: bool = False,
    ):
        """
        Initialize an Archive object.

        Args:
            data: representation of email messages with columns for
                Message-ID, From, Date, In-Reply-To, References, and Body.
                The created Archive becomes a wrapper around a copy of
                the input DataFrame.

        Upon initialization, the Archive object drops duplicate entries
        and sorts its member variable *data* by Date.
        """
        self.data = df

        try:
            self.data["Date"] = pd.to_datetime(
                self.data["Date"],
                errors="coerce",
                infer_datetime_format=True,
                utc=True,
            )
        except Exception as e:
            logging.error(
                "Error while converting to datetime, despite coerce mode. "
                + f"The error message is: {e}"
            )
            raise ArchiveWarning(
                "Error while converting to datetime, despite coerce mode. "
                + f"The error message is: {e}"
            )

        try:
            self.data.drop_duplicates(inplace=True)
        except Exception as e:
            logging.error(
                "Error while removing duplicate messages, maybe timezone issues?"
                + f"The error message is: {e}",
                exc_info=True,
            )

        # Drops any entries with no Date field.
        # It may be wiser to optionally
        # do interpolation here.
        if self.data["Date"].isnull().any():
            # self.data.dropna(subset=['Date'], inplace=True)
            self.data = self.data[self.data["Date"].notnull()]
            # workaround for https://github.com/pandas-dev/pandas/issues/13407

        # convert any null fields to None -- csv saves these as nan sometimes
        self.data = self.data.where(pd.notnull(self.data), None)

        try:
            # set the index to be the Message-ID column
            self.data.set_index("Message-ID", inplace=True)
        except KeyError:
            # will get KeyError if Message-ID is already index
            pass

        # let's do a pass to try to find bad tzinfo's
        bad_indices = []
        for i, row in self.data.iterrows():
            try:
                row["Date"] < datetime.datetime.now(pytz.utc)
            except Exception as e:
                logging.error(
                    "Error timezone issues while detecting bad rows. "
                    + f"The error message is: {e}",
                    exc_info=True,
                )
                bad_indices.append(i)
                logging.info("Bad timezone on %s", row["Date"])
        if len(bad_indices) > 0:
            # drop those rows that threw an error
            self.data = self.data.drop(bad_indices)
            logging.info("Dropped %d rows", len(bad_indices))

        try:
            self.data.sort_values(by="Date", inplace=True)
        except Exception as e:
            logging.error(
                "Error while sorting, maybe timezone issues?"
                + f" The error message is: {e}",
                exc_info=True,
            )
            raise ArchiveWarning(
                "Error while sorting, maybe timezone issues?"
                + f" The error message is: {e}",
            )

        if self.data.empty:
            raise mailman.MissingDataException(
                "Archive after initial processing is empty. Was data collected properly?"
            )

    @classmethod
    def from_url(cls, url: str) -> "Archive":
        """
        Initialize class from url link.
        """
        assert url.startswith("http://") or url.startswith("https://")
        df = mailman.collect_from_url(url)
        return cls.from_dataframe(df)

    @classmethod
    def from_files(
        cls,
        dir_paths: List[str],
        file_names: List[str],
        email_list_software: Optional[str] = None,
    ) -> "Archive":
        """
        Initialize class from multiple files.

        Args:
            file_path: String is interpreted as a path to either a:
                - pd.DataFrame stored as .csv or .h5
                - single .mbox file
                - a directory of .mbox files (also in .mbox format).
                Note that the last two cases file extensions need not be .mbox;
                frequently they will be .txt.
            email_list_software: Can be one of the following strings:
                - GNU-Mailman
                - LISTSERV
                to indicate whether the file(s) are in that format.
        """
        if email_list_software == "GNU-Mailman":
            df = mailman.open_list_archives(
                file_names[0], archive_dir=dir_paths[0]
            )
        elif email_list_software == "LISTSERV":
            df = listserv.from_files(dir_paths, file_names)
        return cls.from_dataframe(df)

    @classmethod
    def from_file(
        cls,
        file_path: str,
        mbox: bool = False,
        email_list_software: Optional[str] = None,
    ) -> "Archive":
        """
        Initialize class from one file.

        Args:
            file_path: String is interpreted as a path to either a:
                - pd.DataFrame stored as .csv or .h5
                - single .mbox file
                - a directory of .mbox files (also in .mbox format).
                Note that the last two cases file extensions need not be .mbox;
                frequently they will be .txt.
            mbox:
            email_list_software: Can be one of the following strings:
                - GNU-Mailman
                - LISTSERV
                to indicate whether the file(s) are in that format.
        """
        file_extension = file_path.split(".")[-1]
        if file_extension == "h5":
            df = pd.read_hdf(file_path, key="df")
        elif file_extension == "csv":
            df = pd.read_csv(file_path)
        else:
            file_name = file_path.split("/")[-1]
            dir_path = file_path.rstrip(file_name)
            if email_list_software == "GNU-Mailman":
                print(file_name, dir_path)
                df = mailman.load_data(
                    file_name, archive_dir=dir_path, mbox=mbox
                )
            elif email_list_software == "LISTSERV":
                df = listserv.from_file(dir_path, file_name)
        return cls.from_dataframe(df)

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> "Archive":
        """
        Initialize class from file.

        Args:
            df: representation of email messages with columns for
                Message-ID, From, Date, In-Reply-To, References, and Body.
                The created Archive becomes a wrapper around a copy of
                the input DataFrame.
        """
        return cls(df)

    def resolve_entities(self, inplace: bool = True) -> pd.DataFrame:
        """Return data with resolved entities."""
        if self.entities is None:
            if self.activity is None:
                self.get_activity()

            self.entities = process.resolve_sender_entities(self.activity)

        to_replace = []
        value = []

        for e, names in list(self.entities.items()):
            for n in names:
                to_replace.append(n)
                value.append(e)

        data = self.data.replace(
            to_replace=to_replace, value=value, inplace=inplace
        )

        # clear and replace activity with resolved activity
        self.activity = None
        self.get_activity()

        if inplace:
            return self.data
        else:
            return data

    def get_activity(self, resolved: bool = False) -> pd.DataFrame:
        """
        Get the activity matrix of an Archive.

        Args:
            If *resolved* is true, then default entity resolution is run on the
            activity matrix before it is returned.

        Returns:
            Columns of the returned DataFrame are the Senders of emails.
            Rows are indexed by ordinal date.
            Cells are the number of emails sent by each sender on each data.
        """
        if self.activity is None:
            self.activity = self.compute_activity(self)

        if resolved:
            self.entities = process.resolve_sender_entities(self.activity)
            eact = utils.repartition_dataframe(self.activity, self.entities)

            return eact

        return self.activity

    def compute_activity(self, clean: bool = True) -> pd.DataFrame:
        """Return the computed activity."""
        mdf = self.data

        if clean:
            # unnecessary?
            if mdf["Date"].isnull().any():
                mdf = mdf.dropna(subset=["Date"])

            mdf = mdf[
                mdf["Date"] < datetime.datetime.now(pytz.utc)
            ]  # drop messages apparently in the future

        mdf2 = mdf.reindex(columns=["From", "Date"])
        mdf2["Date"] = mdf["Date"].apply(lambda x: x.toordinal())

        activity = (
            mdf2.groupby(["From", "Date"]).size().unstack("From").fillna(0)
        )
        new_date_range = np.arange(mdf2["Date"].min(), mdf2["Date"].max())
        # activity.set_index('Date')
        activity = activity.reindex(new_date_range, fill_value=0)

        return activity

    def get_threads(self, verbose: bool = False) -> list:
        """
        Get threads.

        Args:
        Returns:
        """

        if self.threads is not None:
            return self.threads

        df = self.data

        threads = list()
        visited = dict()

        total = df.shape[0]
        c = 0

        for i in df.iterrows():

            if verbose:
                c += 1
                if c % 1000 == 0:
                    print("Processed %d of %d" % (c, total))

            if i[1]["In-Reply-To"] == "None":
                root = Node(i[0], i[1])
                visited[i[0]] = root
                threads.append(Thread(root))
            elif i[1]["In-Reply-To"] not in list(visited.keys()):
                root = Node(i[1]["In-Reply-To"])
                succ = Node(i[0], i[1], root)
                root.add_successor(succ)
                visited[i[1]["In-Reply-To"]] = root
                visited[i[0]] = succ
                print("add new thread", i[1]["In-Reply-To"])
                threads.append(Thread(root, known_root=False))
            else:
                parent = visited[i[1]["In-Reply-To"]]
                node = Node(i[0], i[1], parent)
                parent.add_successor(node)
                visited[i[0]] = node

        self.threads = threads

        return threads

    def save(self, path: str, encoding: str = "utf-8") -> None:
        """Save data to csv file."""
        self.data.to_csv(path, ",", encoding=encoding)


def find_footer(
    messages: Union[np.ndarray, pd.DataFrame], number: int = 1
) -> np.ndarray:
    """
    Returns the footer of a DataFrame of emails.

    A footer is a string occurring at the tail of most messages.
    Messages can be a DataFrame or a Series
    """
    if isinstance(messages, pd.DataFrame):
        messages = messages["Body"]

    # sort in lexical order of reverse strings to maximize foot length
    srb = messages.apply(lambda x: None if x is None else x[::-1]).order()
    # srb = df.apply(lambda x: None if x['Body'] is None else x['Body'][::-1],
    #              axis=1).order()
    # begin walking down the series looking for maximal overlap
    counts = {}

    last = None

    def clean_footer(foot):
        return foot.strip()

    for b in srb:
        if last is None:
            last = b
            continue
        elif b is None:
            continue
        else:
            head, i = utils.get_common_head(b, last, delimiter="\n")
            head = clean_footer(head[::-1])
            last = b

            if head in counts:
                counts[head] = counts[head] + 1
            else:
                counts[head] = 1

        last = b

    # reduce candidates that are strictly longer and less frequent
    # than most promising footer candidates
    for n, foot1 in sorted(
        [(v, k) for k, v in list(counts.items())], reverse=True
    ):
        for foot2, m in list(counts.items()):
            if n > m and foot1 in foot2 and len(foot1) > 0:
                counts[foot1] = counts[foot1] + counts[foot2]
                del counts[foot2]

    candidates = sorted(
        [(v, k) for k, v in list(counts.items())], reverse=True
    )

    return candidates[0:number]
