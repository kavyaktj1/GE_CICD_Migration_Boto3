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
    A function to fetch the Tables details.

    :param glue_crawler_name:
    :return:
    """
    try:
        result_response = {}
        crawler_metrics_response = GLUE_CLIENT.get_crawler_metrics(CrawlerNameList=[glue_crawler_name])
        metrics_response = crawler_metrics_response['CrawlerMetricsList'][0]

        if not metrics_response['StillEstimating']:

            print('Metrics response {}'.format(crawler_metrics_response['CrawlerMetricsList'][0]))

            result_response.update({'TablesCreated': metrics_response['TablesCreated']})
            result_response.update({'TablesUpdated': metrics_response['TablesUpdated']})
            result_response.update({'TablesDeleted': metrics_response['TablesDeleted']})

            create_delete_message = 'You have received this notification because ' \
                                    + str(metrics_response['TablesCreated']) + ' Tables Created, ' \
                                    + str(metrics_response['TablesDeleted']) + ' Tables Deleted in following Crawler : '\
                                    + glue_crawler_name

            updated_message = 'You have received this notification because ' \
                              + str(metrics_response['TablesUpdated']) + ' tables are updated in following Crawler : '\
                              + glue_crawler_name

            result_response.update({'TablesUpdatedMessage': updated_message})
            result_response.update({'CreateDeleteMessage': create_delete_message})

        return result_response

    except Exception as crawler_error:
        print('Error while crawling metrics {}'.format(crawler_error))
        return None


def lambda_handler(event, context):

    print('input payload {}'.format(event))
    CRAWLER_NAME = event['CRAWLER_NAME']

    last_cralwer_runtime = get_glue_crawler_metrics(CRAWLER_NAME)

    if last_cralwer_runtime is not None:
        return last_cralwer_runtime

    else:
        raise CrawlerAlreadyRunning('AlreadyRunning')
