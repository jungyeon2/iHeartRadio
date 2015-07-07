__author__ = 'jungyeonyoon'

import luigi
import os
import random
import urllib

# from os import listdir
class Input_Generator(luigi.Task):
    date = luigi.DateParameter()

    def output(self):
        return luigi.LocalTarget("/Users/jungyeonyoon/Documents/iHeartRadio/Luigi/examples/input/input_{}.tsv".format(self.date))

    def run(self):
         with self.output().open('w') as output:
            for _ in range(1000):
                output.write('{}\n'.format(random.randint(0,999)))


class Sum(luigi.Task):
    date_interval = luigi.DateIntervalParameter()

    def requires(self):
        return [Input_Generator(date) for date in self.date_interval]
        # return Input_Generator(self.date_interval)

    def output(self):
        return luigi.LocalTarget("/Users/jungyeonyoon/Documents/iHeartRadio/Luigi/examples/output/sum.tsv")


    def run(self):
        cnt = 0
        try:

            path = "/Users/jungyeonyoon/Documents/iHeartRadio/Luigi/examples/input"
            for dir_entry in os.listdir(path):

                if not dir_entry.startswith('.'):

                    dir_entry_path = os.path.join(path, dir_entry)
                    if os.path.isfile(dir_entry_path):
                        with open(dir_entry_path, 'r') as my_file:


                            for i in my_file.readlines():
                                cnt += int(i)

                    print cnt

        except ValueError:
            pass

        with self.output().open('w') as out_file:
            out_file.write(str(cnt))



class Count(luigi.Task):

    date_interval = luigi.DateIntervalParameter()

    # def requires(self):
    #     return Sum(self.date_interval)


    def output(self):
        return luigi.LocalTarget("/Users/jungyeonyoon/Documents/iHeartRadio/Luigi/examples/output/count.tsv")

    def run(self):
        cnt = 0

        path = "/Users/jungyeonyoon/Documents/iHeartRadio/Luigi/examples/input"
        for dir_entry in os.listdir(path):
            if not dir_entry.startswith('.'):
                dir_entry_path = os.path.join(path, dir_entry)
                if os.path.isfile(dir_entry_path):

                    with open(dir_entry_path, 'r') as my_file:
                        for i in my_file.readlines():
                            cnt += 1

        with self.output().open('w') as out_file:
            out_file.write(str(cnt))

class Avg(luigi.Task):

    date_interval = luigi.DateIntervalParameter()

    def requires(self):
        return {'sum':Sum(self.date_interval), 'cnt':Count(self.date_interval)}

    def output(self):
        return luigi.LocalTarget("/Users/jungyeonyoon/Documents/iHeartRadio/Luigi/examples/output/avg.tsv")

    def run(self):
        s = 0
        c = 0

        with self.input()['sum'].open('r') as sum_file:
            for line in sum_file:
                s = line
                print 'sum: ', line

        with self.input()['cnt'].open('r') as cnt_file:
            for line in cnt_file:
                c = line
                print 'cnt: ', line


        with self.output().open('w') as out_file:
            out_file.write('The average is {}\n'.format(float(s)/float(c)))
        # for t in self.input():
        #
        #     with t.open('r')['sum'] as in_file:
        #         for line in in_file:
        #             print 'sum: ', line
        #             # _, artist, track = line.strip().split()
        #             # artist_count[artist] += 1
        #
        # with self.output().open('w') as out_file:
        #     for artist, count in six.iteritems(artist_count):
        #         out_file.write('{}\t{}\n'.format(artist, count))

if __name__ == '__main__':
    luigi.run()