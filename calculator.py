#!/usr/bin/env python3

import sys,getopt
import configparser
import queue
from multiprocessing import Process, Queue
from collections import namedtuple
from datetime import datetime
import csv


IncomeTaxQuickLookupItem = namedtuple(
    'IncomeTaxQuickLookupItem',
    ['start_point', 'tax_rate', 'quick_subtractor']
)

INCOME_TAX_START_POINT = 3500

INCOME_TAX_QUICK_LOOKUP_TABLE = [
    IncomeTaxQuickLookupItem(80000, 0.45, 13505),
    IncomeTaxQuickLookupItem(55000, 0.35, 5505),
    IncomeTaxQuickLookupItem(35000, 0.30, 2755),
    IncomeTaxQuickLookupItem(9000, 0.25, 1005),
    IncomeTaxQuickLookupItem(4500, 0.2, 555),
    IncomeTaxQuickLookupItem(1500, 0.1, 105),
    IncomeTaxQuickLookupItem(0, 0.03, 0)
]

q_user = Queue()
q_result = Queue()

class Args():

    def __init__(self):
        self.options = self._options()
        self.city = self.options.get('-C')
        self.cfg = self.options.get('-c')
        self.user = self.options.get('-d')
        self.export = self.options.get('-o')

    def _options(self):
        try:
            shortargs = 'hC:c:d:o:'
            longargs = ['help']
            opts,_ = getopt.getopt(sys.argv[1:], shortargs, longargs)
        except getopt.GetoptError:
            print('Parameter Error,Please try "-h"')
            exit()
        options = dict(opts)
        if len(options) == 1 and ('-h' in options or '--help' in options):
            print('Usage: calculator.py -C cityname -c configfile -d userdata -o resultdata')
            exit()
        return options

args = Args()

class Config():

    def __init__(self):
        self.config = self._read_config()

    def _read_config(self):
        config = configparser.ConfigParser()
        config.read(args.cfg)
        if args.city or args.city.upper() in config.sections():
            print('args.city:',args.city)
            self.social_insurance_baseline_low = float(config.get(args.city.upper(),'JiShuL'))
            self.social_insurance_baseline_high = float(config.get(args.city.upper(),'JiShuH'))
            self.social_insurance_total_rate = sum([float((config.get(args.city.upper(),'YangLao'))),float((config.get(args.city.upper(),'YiLiao'))),float((config.get(args.city.upper(),'ShiYe'))),float((config.get(args.city.upper(),'GongShang'))),float((config.get(args.city.upper(),'ShengYu'))),float((config.get(args.city.upper(),'GongJiJin')))])
        print('rate',self.social_insurance_total_rate)
        return self.social_insurance_baseline_low,self.social_insurance_baseline_high

config = Config()
        
class Userdata():
    def __init__(self):
        self.userdata = self._read_userdata()
    def _read_userdata(self):
        print('args.user:',args.user)
        with open(args.user) as f:
            for line in f.readlines():
                employeeid,income_str = line.strip().split(',')
                try:
                    income = int(income_str)
                except:
                    print('Parameter Error')
                    exit()
                yield (employeeid,income)
    def run(self):
        for data in self.userdata:
            q_user.put(data)
            print('quser:',q_user)

class IncomeTaxCalculator(Process):
    @staticmethod
    def social_insurance_cal(income):
        if income < config.social_insurance_baseline_low:
            return config.social_insurance_baseline_low * config.social_insurance_total_rate
        elif income > config.social_insurance_baseline_high:
            return config.social_insurance_baseline_high * config.social_insurance_total_rate
        else:
            return income * config.social_insurance_total_rate
    @classmethod
    def calc_income_tax_and_remain(cls,income):
        real_income = income - cls.social_insurance_cal(income)
        tax_part = real_income - INCOME_TAX_START_POINT
        if tax_part <= 0:
            return '0.00', '{:.2f}'.format(real_income)
        for item in INCOME_TAX_QUICK_LOOKUP_TABLE:
            if tax_part > item.start_point:
                tax = tax_part * item.tax_rate - item.quick_subtractor
                return '{:.2f}'.format(tax),'{:.2f}'.format(real_income - tax)
    def calc_for_all_userdata(self):
        while True:
            try:
                employeeid,income = q_user.get(timeout=1)
            except queue.Empty:
                print('queue is exception')
                exit()
            data = [employeeid,income]
            social_insurance_money = '{:2f}'.format(self.social_insurance_cal(income))
            tax,remain = self.calc_income_tax_and_remain(income)
            data.extend([social_insurance_money,tax,remain])
            data.append(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            yield data
    def run(self):
        for data in self.calc_for_all_userdata():
            q_result.put(data)

class Exporter(Process):
    def run(self):
        with open(args.export,'w',newline='') as f:

            while True:
                writer = csv.writer(f)
                try:
                    usrdata = q_result.get(timeout=1)
                except queue.Empty:
                    exit()
                writer.writerows(usrdata)


if __name__ == '__main__':
    workers = [
        Userdata(),
        IncomeTaxCalculator(),
        Exporter()
    ]
    for worker in workers:
        worker.run()



