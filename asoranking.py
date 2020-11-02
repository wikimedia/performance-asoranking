#!/usr/bin/env python3

import argparse
import datetime
import logging
import os
from shutil import copyfile
import subprocess
import tempfile

import geoip2.database
import numpy
import pandas


class ASORanking:
    country_names = {
        'US': 'United States',
        'DE': 'Germany',
        'JP': 'Japan',
        'GB': 'United Kingdom',
        'FR': 'France',
        'RU': 'Russian Federation',
        'IN': 'India',
        'IT': 'Italy',
        'CA': 'Canada',
        'BR': 'Brazil',
        'ES': 'Spain',
        'PL': 'Poland'
    }

    def run(self):
        self.parge_args()
        self.setup_logging()
        self.generate_report()

    def setup_logging(self):
        FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        logging.basicConfig(format=FORMAT)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(self.args.loglevel)

    def parge_args(self):
        today = datetime.date.today()
        first = today.replace(day=1)
        lastMonth = first - datetime.timedelta(days=1)

        parser = argparse.ArgumentParser(description='Generate ISP ranking.')
        parser.add_argument('--threshold', type=int, default=500, help='sample size threshold')
        parser.add_argument('--countries', default=ASORanking.country_names.keys(), nargs='+', help='countries')
        parser.add_argument('--year', type=int, default=lastMonth.year, help='year')
        parser.add_argument('--month', type=int, default=lastMonth.month, help='month')
        parser.add_argument('--cpu-span', type=int, default=100, help='size of CPU score span around median to keep')
        parser.add_argument(
            '--debug',
            action='store_const',
            dest='loglevel',
            const=logging.DEBUG,
            default=logging.ERROR,
            help='display debug logging'
        )
        parser.add_argument('--publish', action='store_true', help='publish dataset')
        self.args = parser.parse_args()

    def fetch_sql(self, sql):
        self.logger.debug('Running %s' % sql)

        sql_fd, sql_path = tempfile.mkstemp()
        tsv_fd, tsv_path = tempfile.mkstemp()

        with open(sql_path, 'w') as f:
            f.write(sql)

        command = [
            '/usr/local/bin/beeline',
            '--outputformat=tsv2',
            '--silent=true',
            '-f',
            sql_path
        ]
        p = subprocess.Popen(command, stdout=tsv_fd)
        p.communicate()

        return pandas.read_csv(tsv_path, error_bad_lines=False, low_memory=False, sep='\t')

    def fetch_cpu_benchmark_medians(self, year, month):
        # Get Median CPU score per country for cellular connectivity + mobile site
        cellular_sql = """SELECT nt.event.originCountry AS country, PERCENTILE(cb.event.score, 0.5) AS score
            FROM event.CpuBenchmark AS cb JOIN event.NavigationTiming AS nt
            ON cb.event.pageviewToken = nt.event.pageviewToken
            WHERE cb.year = %d AND cb.month = %d AND nt.year = %d AND nt.month = %d
            AND nt.event.netinfoConnectionType  = 'cellular' AND nt.event.mobileMode = 'stable'
            GROUP BY nt.event.originCountry;""" % (year, month, year, month)
        cellular = self.fetch_sql(cellular_sql)

        # Get Median CPU score per country for wifi connectivity + desktop site
        wifi_sql = """SELECT nt.event.originCountry AS country, PERCENTILE(cb.event.score, 0.5) AS score
            FROM event.CpuBenchmark AS cb JOIN event.NavigationTiming AS nt
            ON cb.event.pageviewToken = nt.event.pageviewToken
            WHERE cb.year = %d AND cb.month = %d AND nt.year = %d AND nt.month = %d
            AND nt.event.netinfoConnectionType  = 'wifi' AND nt.event.mobileMode IS NULL
            GROUP BY nt.event.originCountry;""" % (year, month, year, month)
        wifi = self.fetch_sql(wifi_sql)

        return cellular, wifi

    def fetch_navigationtiming_data(self, country, year, month):
        self.logger.debug('Fetching navtiming dataset')

        sql = """SELECT useragent.device_family, ip, event.responseStart - event.connectStart AS ttfb,
            event.loadEventStart - event.responseStart AS plt, event.netinfoConnectionType AS type,
            event.pageviewToken, event.transferSize, event.mobileMode FROM event.NavigationTiming
            WHERE year = %d AND month = %d AND event.originCountry = \'%s\'
            SORT BY RAND() LIMIT 1000000;""" % (year, month, country)

        navtiming_dataset = self.fetch_sql(sql)

        self.logger.debug('Processing navtiming dataset')

        navtiming_dataset = navtiming_dataset.drop_duplicates(['pageviewtoken'], keep='first')
        navtiming_dataset = self.add_isp(navtiming_dataset)

        return navtiming_dataset

    def fetch_and_combine_cpubenchmark(self, navtiming_dataset, country, year, month, mincpu, maxcpu):
        self.logger.debug('Fetching cpubenchmark dataset')

        sql = """SELECT nt.ip, nt.event.pageviewToken, cb.event.score FROM event.NavigationTiming AS nt
            INNER JOIN event.CpuBenchmark AS cb ON nt.event.pageviewToken = cb.event.pageviewToken
            WHERE nt.year = %d AND nt.month = %d AND cb.year = %d AND cb.month = %d
            AND nt.event.originCountry = \'%s\' AND cb.event.score > %d
            AND cb.event.score < %d;""" % (year, month, year, month, country, mincpu, maxcpu)

        cpu_dataset = self.fetch_sql(sql)

        self.logger.debug('Processing cpubenchmark dataset')

        # Sanitizing, we only want one CPU benchmark score per pageviewtoken
        cpu_dataset = cpu_dataset.drop_duplicates(['pageviewtoken'], keep='first')
        cpu_dataset = self.add_isp(cpu_dataset)

        self.logger.debug('Only keep navtiming records that have a corresponding cpu benchmark entry')
        navtiming_dataset = navtiming_dataset.loc[navtiming_dataset['pageviewtoken'].isin(cpu_dataset['pageviewtoken'])]

        return navtiming_dataset, cpu_dataset

    def add_isp(self, dataset):
        self.logger.debug('Looking up and adding ISP info for each IP address in dataset')
        asns = []
        asos = []
        with geoip2.database.Reader('/usr/share/GeoIP/GeoIP2-ISP.mmdb') as reader:
            for ip in dataset['ip']:
                try:
                    response = reader.isp(ip)
                    asn = int(response.autonomous_system_number)
                    aso = response.autonomous_system_organization
                except (geoip2.errors.AddressNotFoundError, TypeError, ValueError):
                    self.logger.debug('Could not determine ASN/ASO for ip address %r' % ip)
                    asn = 0
                    aso = ''

                asns.append(asn)
                asos.append(aso)

        dataset['asn'] = asns
        dataset['aso'] = asos

        return dataset

    def get_asns_by_type(self, navtiming_dataset, network):
        # We consider mobile ASNs to be ones where "cellular" has been seen at least once
        # and desktop ASNs to be the ones where "wifi" has been seen at least once

        cellular_asns = navtiming_dataset[navtiming_dataset.type == 'cellular'].asn.unique()

        if network == 'cellular':
            return cellular_asns
        else:
            return navtiming_dataset[navtiming_dataset.type == 'wifi'].asn.unique()

    def generate_ranking(
            self,
            navtiming_dataset,
            year,
            month,
            cpu_span,
            country,
            network,
            cellular_medians,
            wifi_medians,
            threshold):
        if network == 'cellular':
            medians_for_country = cellular_medians[cellular_medians.country == country]
            mediancpu = medians_for_country['score'].item()
        else:
            mediancpu = wifi_medians[wifi_medians.country == country]['score'].item()

        self.logger.debug('Median CPU benchmark score for %s %s: %f' % (country, network, mediancpu))
        mincpu = mediancpu - cpu_span / 2
        maxcpu = mediancpu + cpu_span / 2

        navtiming_dataset_filtered, cpubenchmark_dataset = self.fetch_and_combine_cpubenchmark(
            navtiming_dataset,
            country,
            year,
            month,
            mincpu,
            maxcpu
        )

        # Find out which ASNs correspond to this network type, thanks to records that have a connection type
        whitelisted_asns = self.get_asns_by_type(navtiming_dataset, network)

        self.logger.debug('%d whitelisted ASNs for %s' % (len(whitelisted_asns), network))

        # Only keep data for those ASNs, which may include records that don't have a connection type set
        navtiming_dataset = navtiming_dataset[navtiming_dataset.asn.isin(whitelisted_asns)]

        # Filter out pageviews that are of the mobile site on a desktop provider and vice versa
        # Since comparing mobile and desktop pageviews is mixing apples and oranges, particularly on page weight
        # We also filter out the alpha and beta mobile sites while we're at it
        if network == 'cellular':
            navtiming_dataset = navtiming_dataset[navtiming_dataset.mobilemode == 'stable']
        else:
            navtiming_dataset = navtiming_dataset[~navtiming_dataset.mobilemode.isin(['stable', 'alpha', 'beta'])]

        # From this point forward, we calculate metrics per ASO, as one ASO might have multiple ASNs
        median_ttfb_by_aso = navtiming_dataset.groupby(navtiming_dataset.aso)[['ttfb']].median()

        median_cpubenchmark_by_aso = cpubenchmark_dataset.groupby(cpubenchmark_dataset.aso)[['score']].median()

        median_plt_by_aso = navtiming_dataset.groupby(navtiming_dataset.aso)[['plt']].median()
        median_transfersize_by_aso = navtiming_dataset.groupby(navtiming_dataset.aso)[['transfersize']].median()
        median_ttfb_by_aso = median_ttfb_by_aso.sort_values(by='ttfb')

        final_ranking = []

        for aso, median_ttfb in median_ttfb_by_aso.itertuples():
            just_this_aso_cpubenchmark = cpubenchmark_dataset.loc[cpubenchmark_dataset.aso == aso]

            if just_this_aso_cpubenchmark.shape[0] < threshold:
                continue

            median_cpu_score = 0

            for cpu_aso, median in median_cpubenchmark_by_aso.itertuples():
                if cpu_aso == aso:
                    median_cpu_score = median

            median_plt = 0

            for plt_aso, median in median_plt_by_aso.itertuples():
                if plt_aso == aso:
                    median_plt = median

            median_transfersize = 0

            for plt_aso, median in median_transfersize_by_aso.itertuples():
                if plt_aso == aso:
                    median_transfersize = median
                    if numpy.isnan(median):
                        # Not all records have transferSize.
                        # There's a small chance an ASO would only have records without one
                        median_transfersize = 0

            final_ranking.append((
                aso,
                int(median_ttfb),
                int(median_plt),
                int(median_cpu_score),
                int(median_transfersize),
                just_this_aso_cpubenchmark.shape[0]
            ))

        return final_ranking

    def generate_report(self):
        year = self.args.year
        month = self.args.month
        countries = self.args.countries
        tsv_headers = [
            u'Country',
            u'Country code',
            u'Type',
            u'ASO',
            u'TTFB',
            u'PLT',
            u'CPU',
            u'Transfer size',
            u'Sample size'
        ]
        tsv_header = u'\u0009'.join(tsv_headers) + u'\u000A'

        filepath = '%d-%02d.tsv' % (year, month)
        publish_path = '/srv/published-datasets/performance/autonomoussystems/'

        if self.args.publish:
            filepath = os.path.join(publish_path, filepath)

        with open(filepath, 'w') as f:
            f.write(tsv_header)
            networks = ['cellular', 'wifi']

            self.logger.debug('Getting CPU medians per country per network type')
            cellular_medians, wifi_medians = self.fetch_cpu_benchmark_medians(year, month)

            for country in countries:
                navtiming_dataset = self.fetch_navigationtiming_data(country, year, month)

                for network in networks:
                    ranking = self.generate_ranking(
                        navtiming_dataset,
                        year,
                        month,
                        self.args.cpu_span,
                        country,
                        network,
                        cellular_medians,
                        wifi_medians,
                        self.args.threshold
                    )
                    country_name = ASORanking.country_names[country]
                    label = 'Mobile' if network == 'cellular' else 'Desktop'

                    for ranking_tuple in ranking:
                        self.logger.debug(
                            'Writing tuple: %r for %s %s %s' % (
                                ranking_tuple,
                                country_name,
                                country,
                                label
                            )
                        )

                        f.write(
                            (u'%s\t%s\t%s\t%s\n' % (
                                country_name,
                                country,
                                label,
                                u'\u0009'.join(map(lambda x: str(x), ranking_tuple))
                            ))
                        )

        print('Dataset written to %s' % filepath)

        if self.args.publish:
            latest_path = os.path.join(publish_path, 'latest.tsv')
            copyfile(filepath, latest_path)
            print('And copied to %s' % latest_path)


if __name__ == '__main__':
    aso = ASORanking()
    aso.run()
