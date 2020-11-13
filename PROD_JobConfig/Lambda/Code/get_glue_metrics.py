import json
import boto3

GLUE_CLIENT = boto3.client('glue')


class CrawlerAlreadyRunning(Exception):
    """
    Custom exception to retry from Step function.
    """
    pass


def get_glue_crawler_metrics(glue_crawler_name):
    """
    A function to get the MedianTime from the given crawler name
    :param glue_crawler_name:
    :return:
    """
    try:
        result_response = ''
        crawler_metrics_response = GLUE_CLIENT.get_crawler_metrics(CrawlerNameList=[glue_crawler_name])
        metrics_response = crawler_metrics_response['CrawlerMetricsList'][0]
        if not metrics_response['StillEstimating']:
            print('Metrics response {}'.format(crawler_metrics_response['CrawlerMetricsList'][0]))
            result_response = round(metrics_response['MedianRuntimeSeconds'])
            
        # print('result {}'.format(result_response))

        return result_response

    except Exception as crawler_error:
        print('Error while crawling metrics {}'.format(crawler_error))
        return None


def lambda_handler(event, context):
    print('input payload {}'.format(event))
    CRAWLER_NAME = event['CRAWLER_NAME']

    last_cralwer_runtime = get_glue_crawler_metrics(CRAWLER_NAME)

    if (last_cralwer_runtime is not None) and (last_cralwer_runtime != ''):
        return {"LAST_MEDIAN_IN_SEC": last_cralwer_runtime}
    else:
        raise CrawlerAlreadyRunning('AlreadyRunning')
