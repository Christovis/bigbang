from nose.tools import *
import bigbang.parse as parse
import os


test_txt = ""

def setup():
    pass
    
def teardown():
    pass

@with_setup(setup, teardown)
def test_parse():

    dirname = os.path.dirname(os.path.abspath(__file__))
    test_file_name = os.path.join(dirname,"2001-November.txt")

    messages = parse.open_mail_archive(test_file_name)

    for message in messages:
        assert len(message.items()) <= 6, "too many fields:\n%s" % (message)

def test_split_references():
    refs = " <ye1y9ljtxwk.fsf@orange30.ex.ac.uk>\n\t<055701c16727$b57fed90$8fd6afcf@pixi.com>"
    split = parse.split_references(refs)
    assert len(split) == 2, split