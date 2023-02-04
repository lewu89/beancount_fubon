import re
from datetime import datetime
from beancount.ingest import importer
from beancount.core.data import Posting, Transaction, Balance, EMPTY_SET, new_metadata
from beancount.core.amount import Amount
from beancount.core.number import D
from beancount.core.flags import FLAG_OKAY
from subprocess import PIPE, Popen


def pdftotext(filename):
    """Convert a PDF file to a text equivalent.
    Args:
    filename: A string path, the filename to convert.
    Returns:
    A string, the text contents of the filename.
    """
    pipe = Popen(['pdftotext', '-enc', 'UTF-8', '-layout',
                 filename, '-'], stdout=PIPE, stderr=PIPE)
    stdout, _stderr = pipe.communicate()
    return stdout.decode()


class Importer(importer.ImporterProtocol):

    def __init__(self, account="Liabilities:Fubon"):
        self.account = account

    def identify(self, file):
        if file.mimetype() != 'application/pdf':
            return False
        text = file.convert(pdftotext)
        return '台北富邦銀行' in text and '本期應繳總額' in text

    def file_name(self, file):
        return "fubon.pdf"

    def file_account(self, file):
        return self.account

    def file_date(self, file):
        text = file.convert(pdftotext)
        pattern = re.compile(
            r'帳單結帳日.*\n.*(?P<date1>\d\d\d/\d\d/\d\d)\s+(?P<date2>\d\d\d/\d\d/\d\d)')
        match = re.search(pattern, text)['date1'].split('/')
        year = match[0]
        month = match[1]
        day = match[2]
        year = int(year) + 1911
        return datetime.strptime(f'{year}{month}{day}', "%Y%m%d").date()

    def extract(self, file):
        acct = self.file_account(file)
        text = file.convert(pdftotext)
        entries = []
        # All entries
        pattern = re.compile(
            r'(?P<consume_date>\d\d\d/\d\d/\d\d)\s+(?P<description>.*)(?P<bill_date>\d\d\d/\d\d/\d\d)\s+(?P<currency>TWD)\s+(?P<amount>[^\s]+)$', re.MULTILINE)
        matches = re.findall(pattern, text)
        for (consume_date, desc, bill_date, currency, amount) in matches:
            desc = desc.strip()
            # date = 110/04/20, need to convert
            [y, m, d] = consume_date.split('/')
            y = int(y) + 1911
            date = datetime.strptime(f"{y}{m}{d}", "%Y%m%d").date()
            amount = Amount(D(amount), "TWD")

            entries.append(Transaction(
                date=date,
                payee=None,
                narration=desc,
                meta=new_metadata(file.name, int(1)),
                flag=FLAG_OKAY,
                tags=EMPTY_SET,
                links=EMPTY_SET,
                postings=[
                    Posting(account=acct, units=None, cost=None,
                            price=None, flag=None, meta=None),
                    Posting(account="TODO", units=amount, cost=None,
                            price=None, flag=None, meta=None)
                ]
            ))

        # Balance
        pattern = re.compile(
            r'本期應繳總額\s+TWD\s+(?P<amount>[^\s]+)$', re.MULTILINE)
        amount = re.search(pattern, text)['amount']
        balance = Amount(D("-" + amount), "TWD")
        entries.append(Balance(
            date=self.file_date(file),
            account=acct,
            amount=balance,
            meta=new_metadata(file.name, int(1)),
            tolerance=0,
            diff_amount=0
        ))
        return entries
