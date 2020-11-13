import json
import boto3
import time

GLUE_CLIENT = boto3.client('glue')


# Custom exception raise
class CrawlerAlreadyRunning(Exception):
    pass


def start_glue_crawler(glue_crawler_name):
    """
    A function to start the crawler using Name.
    :param glue_crawler_name:
    :return:
    """
    try:
        start_response = GLUE_CLIENT.start_crawler(Name=glue_crawler_name)
        print('start crawler response {}'.format(start_response))
    except Exception as startError:
        print('Error while start crawler {}'.format(startError))


def get_glue_crawler_metrics(glue_crawler_name):
    """
    A function to check the crawler current status, if not running will start and get the
    TimeLeftSeconds will return

    :param glue_crawler_name:
    :return:
    """
    try:
        get_response = ''
        crawler_metrics_response = GLUE_CLIENT.get_crawler_metrics(CrawlerNameList=[glue_crawler_name])
        metrics_response = crawler_metrics_response['CrawlerMetricsList'][0]
        if (not metrics_response['StillEstimating']):
            print('Metrics response {}'.format(crawler_metrics_response['CrawlerMetricsList'][0]))

            start_glue_crawler(glue_crawler_name)
            time.sleep(5)
            crawler_metrics_response_new = GLUE_CLIENT.get_crawler_metrics(CrawlerNameList=[glue_crawler_name])
            metrics_response_new = crawler_metrics_response_new['CrawlerMetricsList'][0]
            print('Time left in sec {}'.format(metrics_response_new['TimeLeftSeconds']))
            get_response = round(metrics_response['TimeLeftSeconds'])

        else:
            print('Crawler still running. TimeLeftSeconds{}'.format(metrics_response['TimeLeftSeconds']))
            get_response = round(metrics_response['TimeLeftSeconds'])

        return get_response

    except Exception as crawler_error:
        print('Error while crawling metrics {}'.format(crawler_error))
        return 0


def lambda_handler(event, context):
    print('input payload {}'.format(event))
    CRAWLER_NAME = event['CRAWLER_NAME']

    last_cralwer_runtime = get_glue_crawler_metrics(CRAWLER_NAME)
    if (last_cralwer_runtime is not None) and (last_cralwer_runtime < 1):
        last_cralwer_runtime = 1
        return {"LAST_RUNTIME_IN_SEC": last_cralwer_runtime}
    else:
        raise CrawlerAlreadyRunning('AlreadyRunning')
