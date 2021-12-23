from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

class DateHelper:

    def __init__(self, input_format, output_format, to_string=True):
        self.input_format = input_format
        self.output_format = output_format
        self.to_string = to_string

    def get_relative_date(self, ref_date, years=0, weeks=0, months=0, days=0):
        relative_date = datetime.strptime(ref_date, self.input_format) + relativedelta(years=years, months=months, days=days)

        if self.to_string:
            relative_date = relative_date.strftime(self.output_format)

        return relative_date

    def cal_diff(self, start, end, flag):
        start = datetime.strptime(start, self.input_format)
        end = datetime.strptime(end, self.input_format)
        if flag == 'day':
            diff = (end - start).days
        elif flag == 'month':
            diff = relativedelta(end, start)
            diff = diff.years * 12 + diff.months
        return diff

    def get_date_list(self, start, end):
        diff = self.cal_diff(start, end, flag='day')
        record_list = list()

        for i in range(diff + 1):
            record_list += [self.get_relative_date(start, days=i)]

        return record_list

    def get_month_list(self, start, end):
        diff = self.cal_diff(start, end, flag='month')
        record_list = list()

        for i in range(diff + 1):
            record_list += [self.get_relative_date(start, months=i)]

        return record_list

    def get_month_first_last_date(self, ref_date):
        ref_date = datetime.strptime(ref_date, self.input_format)
        first_date = ref_date.strftime(self.input_format.replace('%d', '01'))
        last_date = self.get_relative_date(first_date, days=-1, months=1)
        return first_date, last_date