"""Analytics Manager module.

This module have a class with built in functionality for Google Analytics.
"""
__author__ = 'Metriplica-Ayyoub'

import time
import os
import json
import re
import hashlib
import logging
import shutil
import pandas as pd
from pykemen.utilities import create_api
from datetime import datetime, timedelta
from googleapiclient.http import MediaFileUpload

logger = logging.getLogger("Analytics")
logger.setLevel(logging.WARNING)

class Analytics(object):
    """Google Analytics class.

    Query unsampled reports from Analytics and cache results.
    Also has a built in method to upload data to Analytics through data import."""
    CACHE_DIR = './cache/{profile}/{id}/'
    CACHE_REPORT = './cache/{profile}/{id}/report_{start_date}_{end_date}.csv'
    CACHE_UNSAMPLED_REPORT = './cache/{profile}/{id}/unsampled_report_{date}.csv'

    class AnalyticsReport(object):
        """"AnalyticsReport class.

        Stores all properties of an Analytics report and returns a dataFrame of the report."""
        REPORT_RE = r"report_[0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{4}-[0-9]{2}-[0-9]{2}\.csv"
        UNSAMPLED_REPORT_RE = r'unsampled_report_[0-9]{4}-[0-9]{2}-[0-9]{2}\.csv'

        def __init__(self, path, start_date, end_date, dimensions, metrics, filters, segments, sort, df, unsampled=False, cache=True):
            """Init method initialize and create AnalyticsReport class.

            Args:
                path (str): path where report files are stored.
                start_date (str): start date of the report (format: %Y-%m-%d)
                end_date (str): end date of the report (format: %Y-%m-%d)
                dimensions (str): comma separated Analytics dimensions
                metrics (str): comma separated Analytics metrics
                filters (str): filters used with analytics api
                segments (str): segments used with analytics api
                sort (str): comma separated dimensions and metrics to sort by the report.

            Returns:
                Analytics.AnalyticsReport: with the given configuration.
            """
            self.path = path
            self.report_re = Analytics.AnalyticsReport.UNSAMPLED_REPORT_RE if unsampled else Analytics.AnalyticsReport.REPORT_RE
            self.start_date = start_date
            self.end_date = end_date
            self.dimensions = dimensions.split(",") if dimensions else None
            self.metrics = metrics.split(",")
            self.filters = filters
            self.segments = segments
            self.sort = sort.split(",")
            self.unsampled = unsampled
            self.cache = cache
            self._df=df

        def to_data_frame(self):
            """Retrieve report into a pandas dataFrame.

            Reads file reports from cache and groups all the required files into a single dataFrame.

            Returns:
                pd.DataFrame
            """
            if not self.cache:
                return self._df
            filenames = os.listdir(self.path)
            filenames = list(filter(lambda x: re.match(self.report_re, x), filenames))
            if self.unsampled:
                filenames = list(filter(lambda x: filter_report_files_by_date(x, self.start_date, self.end_date), filenames))
            else:
                filenames = list(filter(lambda x: x == 'report_{start_date}_{end_date}.csv'.format(
                    start_date=self.start_date, end_date=self.end_date), filenames))
            
            if not len(filenames):
                columns = self.dimensions
                columns.extend(self.metrics)
                return pd.DataFrame(columns=columns)
            filenames.sort()
            dataframes = (pd.read_csv(self.path + filename, index_col=False, dtype=self._get_dtypes()) for filename in
                        filenames)
            dataframe = pd.concat(dataframes, ignore_index=True)
            dataframe = dataframe.groupby(self.dimensions).sum().reset_index()
            for sort in self.sort:
                if sort.startswith('-'):
                    dataframe = dataframe.sort_values(by=[sort[1:]], ascending=False)
                elif sort != '':
                    dataframe = dataframe.sort_values(by=[sort])
            return dataframe

        def to_csv(self, filename):
            """Stores the report into a csv file.

            Args:
                filename (str): path of the filename to store the report into.
            """
            df = self.to_data_frame()
            df.to_csv(filename, encoding='utf-8', index=False)

        def _get_dtypes(self):
            """Returns the types of the report to format correctly the pd.DataFrame.

            Returns:
                dict
            """
            dtypes = {dimension: str for dimension in self.dimensions}
            dtypes.update({metric: float for metric in self.metrics})
            return dtypes

    def __init__(self, credentials=None, secrets=None):
        """Constructor for Analytics class.

        Args:
            credentials(str): json filename with oauth credentials.
            secrets(str): json filename with the access, if there is non, one will be created.

        Returns:
            Analytics
        """
        super(Analytics, self).__init__()
        scope = [
            "https://www.googleapis.com/auth/analytics.edit", 
            "https://www.googleapis.com/auth/analytics",
            "https://www.googleapis.com/auth/analytics.manage.users",
            ]
        self._analyticsService = create_api("analytics", "v3", scope, secrets, credentials)

    def get_report(self, unsampled=False, cache=True, **kwargs):
        """Downloads data from Analytics and caches the result. If the data is alredy cached, skips the
        download and directly returns an Analytics.AnalyticsReport.

        Args:
            unsampled (boolean): True will download the report day by day to try get unsampled data.
            kwargs (**dict): Analytics report configuration variable with all required parameters

        Returns:
            Analytics.AnalyticsReport
        """
        last_hit = 0
        
        
        id_to_hash = ",".join([
            kwargs.get("dimensions", ""), 
            kwargs.get("metrics", ""),
            kwargs.get("filters", ""),
            kwargs.get("segments", ""),
            ])
        id_ = hashlib.md5(id_to_hash.encode('utf8')).hexdigest()
        start_date = kwargs.get('start_date')
        end_date = kwargs.get('end_date')
        columns = ','.join([kwargs.get('dimensions'), kwargs.get('metrics')])
        columns = columns.split(',')
        dtypes = {dimension: str for dimension in kwargs.get("dimensions","").split(",")}
        dtypes.update({metric: float for metric in kwargs.get("metrics", "").split(",")})
        rows = []
        data_frames = []
        if not os.path.isdir(Analytics.CACHE_DIR.format(profile=kwargs.get('ids').replace('ga:', ''), id=id_)):
            os.makedirs(Analytics.CACHE_DIR.format(profile=kwargs.get('ids').replace('ga:', ''), id=id_))
        
        if not unsampled:
            filename = Analytics.CACHE_REPORT.format(
                profile=kwargs.get('ids').replace('ga:', ''), 
                id=id_, 
                start_date=start_date, 
                end_date=end_date
            )
            if not cache or not self._in_cache(kwargs.get('ids').replace('ga:', ''), id_, start_date, end_date):
                report = self._analyticsService.data().ga().get(**kwargs).execute()
                last_hit = time.time()
                if report.get('containSampledData'):
                    logger.warn("There are sampled results on the report: {dimensions}{metrics} - date: {start_date} to {end_date}".format(
                            dimensions=kwargs.get("dimensions"), metrics=kwargs.get("metrics"), start_date=start_date, end_date=end_date))
                rows.extend(report.get('rows', []))
                iteration = 1
                while report.get('nextLink'):
                    try:
                        time.sleep(.1 - (time.time()-last_hit))
                    except ValueError:
                        pass
                    kwargs['start_index'] = 1 + kwargs.get('max_results', 1000) * iteration
                    report = self._analyticsService.data().ga().get(**kwargs).execute()
                    last_hit = time.time()
                    rows.extend(report.get('rows', []))
                    iteration += 1
                df = pd.DataFrame(data=rows, columns=columns)

                if cache:
                    df.to_csv(filename, index=False, encoding='utf-8')
                    logger.info('Saved file' + filename)
        else:
            startDate = datetime.strptime(start_date, "%Y-%m-%d")
            endDate = datetime.strptime(end_date, "%Y-%m-%d")
            diffDays = (endDate - startDate).days + 1
            for day in range(diffDays):
                actualDate = (startDate + timedelta(days=day)).strftime("%Y-%m-%d")
                filename = Analytics.CACHE_UNSAMPLED_REPORT.format(profile=kwargs.get('ids').replace('ga:', ''), id=id_,
                                                        date=actualDate)
                if  not cache or not self._in_cache_by_day(kwargs.get('ids').replace('ga:', ''), id_, actualDate):
                    kwargs["start_date"] = actualDate
                    kwargs["end_date"] = actualDate
                    kwargs["start_index"] = 1
                    report = self._analyticsService.data().ga().get(**kwargs).execute()
                    if report.get("containsSampledData"):
                        logger.warn("There are sampled results on the report: {dimensions}{metrics} - date{date}".format(
                            dimensions=kwargs.get("dimensions"), metrics=kwargs.get("metrics"), date=actualDate))
                    rows.extend(report.get("rows", []))
                    iteration = 1
                    while report.get("nextLink"):
                        try:
                            time.sleep(.1 - (time.time()-last_hit))
                        except ValueError:
                            pass
                        kwargs["start_index"] = 1 + kwargs.get('max_results', 1000) * iteration
                        report = self._analyticsService.data().ga().get(**kwargs).execute()
                        last_hit = time.time()
                        rows.extend(report.get("rows", []))
                        iteration += 1
                    df = pd.DataFrame(data=rows, columns=columns)
                    data_frames.append(df)
                    if cache:
                        df.to_csv(filename, index=False, encoding='utf-8')
                        logger.info("Saved file " + filename)
                    rows = []
            df = pd.concat(data_frames, ignore_index=True)
            df = df.groupby(kwargs.get("dimensions", "").split(",")).sum().reset_index()
            for sort in kwargs.get("sort", "").split(","):
                if sort.startswith('-'):
                    df = df.sort_values(by=[sort[1:]], ascending=False)
                elif sort != '':
                    df = df.sort_values(by=[sort])
        df = df.astype(dtypes)
        return Analytics.AnalyticsReport(
            Analytics.CACHE_DIR.format(profile=kwargs.get('ids', '').replace('ga:', ''), id=id_),
            start_date,
            end_date,
            kwargs.get('dimensions', ''),
            kwargs.get('metrics', ''),
            kwargs.get('filters', ''),
            kwargs.get('segments', ''),
            kwargs.get('sort', ""),
            df,
            unsampled=unsampled,
            cache=cache
        )

    def data_import(self, accountId, webPropertyId, dataSourceId, filename):
        """Import a csv to Analytics through a data import.

        Args:
            accountId (str): Analytics account id to upload the data to
            webPropertyId (str): Property Id where the data import targeted is
            dataSourceId (str): Id of the data import to upload de data to
            filename (str): file path to upload to Analytics

        Returns:
            None if the upload succeed, raise an error otherwise"""
        media = MediaFileUpload(filename, mimetype='application/octet-stream', resumable=False)
        response = self._analyticsService.management().uploads().uploadData(
            accountId=accountId,
            webPropertyId=webPropertyId,
            customDataSourceId=dataSourceId,
            media_body=media).execute()
        while response.get('status') == 'PENDING':
            time.sleep(60)
            response = self._analyticsService.management().uploads().get(
                accountId=accountId,
                webPropertyId=webPropertyId,
                customDataSourceId=dataSourceId,
                uploadId=response.get('id')
            ).execute()
        if response.get('status') == 'FAILED':
            raise Exception(json.dumps(response.get('error'), indent=2))
        if response.get('status') == 'COMPLETED':
            pass

    def _in_cache_by_day(self, profile, id_, date):
        """Check if a specific report is stored in cache.

        Args:
            profile (str): profile id of the report
            id_ (str): hash id of the report
            date (str): date of the report

        Returns:
            bool: True if the report is cached, False otherwise"""
        report_path = Analytics.CACHE_UNSAMPLED_REPORT
        path = report_path.format(profile=profile, id=id_, date=date)
        return os.path.isfile(path)

    def _in_cache(self, profile, id_, start_date, end_date):
        """Check if a specific report is stored in cache.

        Args:
            profile (str): profile id of the report
            id_ (str): hash id of the report
            start_date (str): start date of the report
            end_date (boolean): end date of the report

        Returns:
            bool: True if the report is cached, False otherwise"""
        report_path = Analytics.CACHE_REPORT
        path = report_path.format(profile=profile, id=id_, start_date=start_date, end_date=end_date)
        return os.path.isfile(path)

    def clear_cache(self, id_=None, lifetime=180):
        """Clears cached reports for a given profile with in a given lifetime.

        Args:
            id_ (str): Profile id
            lifetime (int): lifetime in days, by default its 180 days
        """

        if id_ is None:
            shutil.rmtree('./cache')
            return
        today = datetime.now()
        last_day = today - timedelta(days=lifetime)
        for reports in os.listdir('./cache/{id}/'.format(id=id_)):
            for report in os.listdir('./cache/{id}/{reports}/'.format(id=id_, reports=reports)):
                day = datetime.strptime(report, 'report_%Y-%m-%d.csv')
                if day < last_day:
                    os.remove('./cache/{id}/{reports}/{report}'.format(id=id_, reports=reports, report=report))
                    logger.info('Removed cache/{id}/{reports}/{report}'.format(id=id_, reports=reports, report=report))


def filter_report_files_by_date(filename, start_date, end_date):
    """Check if a report file is within a date range.

    Args:
        filename (str): report filename
        start_date (str): start date of the report range
        end_date (str): end date of the report range

    Returns:
        bool: True if the report filename is in the date range, False otherwise"""
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    filename_date = datetime.strptime(filename, "unsampled_report_%Y-%m-%d.csv")
    return start_date <= filename_date and end_date >= filename_date