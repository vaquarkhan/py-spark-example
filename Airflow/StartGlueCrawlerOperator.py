class StartGlueCrawlerOperator(BaseOperator):

    ui_color = '#ff9900'

    @apply_defaults
    def __init__(
            self,
            crawler_name,
            polling_interval=10,
            *args,
            **kwargs
    ):
        """
        Trigger AWS Glue Crawler function

        :param crawler_name (string) [REQUIRED]: the name of the Glue crawler to start and monitor
        :param polling_interval (integer) (default: 10) -- time interval, in seconds, to check the status of the job
        """
        super(StartGlueCrawlerOperator, self).__init__(*args, **kwargs)
        self.crawler_name = crawler_name
        self.polling_interval = polling_interval
        self.glue_client = boto3.client('glue')

    def execute(self, context):

        # Retrieving last_crawl details to compare with later
        last_crawl_before_starting = self.glue_client.get_crawler(
            Name=self.crawler_name)['Crawler']['LastCrawl']

        start_glue_crawler_response = self.glue_client.start_crawler(
            Name=self.crawler_name)
        logging.info("start_glue_crawler Response: " +
                     str(start_glue_crawler_response))

        while True:
            crawler_status = self.glue_client.get_crawler(
                Name=self.crawler_name)['Crawler']['State']
            logging.info("Crawler Status: " + str(crawler_status))

            # Possible values --> 'State': 'READY'|'RUNNING'|'STOPPING'
            if (crawler_status in ['RUNNING']):
                logging.info("Sleeping for " +
                             str(self.polling_interval) + " seconds...")
                time.sleep(self.polling_interval)
            elif (crawler_status in ['STOPPING', 'READY']):
                last_crawl_at_stopping = self.glue_client.get_crawler(Name=self.crawler_name)[
                    'Crawler']['LastCrawl']
                if (last_crawl_before_starting == last_crawl_at_stopping):
                    logging.info("Sleeping for " +
                                 str(self.polling_interval) + " seconds...")
                    time.sleep(self.polling_interval)
                else:
                    final_response = self.glue_client.get_crawler(
                        Name=self.crawler_name)
                    final_status = final_response['Crawler']['LastCrawl']['Status']

                    # Possible values --> 'Status': 'SUCCEEDED'|'CANCELLED'|'FAILED'
                    if (final_status in ['SUCCEEDED']):
                        logging.info("Final Crawler Status: " +
                                     str(final_status))
                        break
                    else:
                        logging.error(
                            "Final Crawler Status: " + str(final_status))
                        logging.error("Message: " + str(final_response.get("Crawler").get(
                            "LastCrawl").get("ErrorMessage", "No Error Message Present")))
                        logging.error(
                            "Check AWS Logs. Exiting.")
                        raise AirflowException('AWS Crawler Job Run Failed')