import argparse
import json
import logging
import os
import threading

import tweepy
from kafka import KafkaProducer


def read_json(file):
    with open(file, 'r') as fp:
        return json.load(fp)


class TwitterProducer(threading.Thread):

    def __init__(self,
                 auth_file,
                 bootstrap_servers,
                 query,
                 lang=None,
                 topic='twitter'):
        """
        Args:
            auth_file: A file path of auth config.
            bootstrap_servers: A 'host[:port]' string of bootstrap server or a list of boostrap servers
            query: A query string.
        """
        threading.Thread.__init__(self)

        self._api = self._get_api(auth_file)

        self.bootstrap_servers = bootstrap_servers
        self.query = query
        self.lang = lang
        self.topic = topic

        self.last_id = 0
        self.logger = logging.getLogger(name='[TwitterProducer]')

    def _get_api(self, auth_path):
        auth_dict = read_json(auth_path)
        auth = tweepy.OAuthHandler(auth_dict['consumer_key'],
                                   auth_dict['consumer_secret'])
        auth.set_access_token(auth_dict['access_token_key'],
                              auth_dict['access_token_secret'])
        return tweepy.API(auth, wait_on_rate_limit=True)

    def run(self):
        self.logger.info('Querying {}.'.format(self.query))

        producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers)

        while True:
            try:
                tweets = self._api.search(
                    self.query,
                    self.lang,
                    since_id=self.last_id,
                    tweet_mode='extended')

                # log number of received tweets
                num_tweets = len(tweets)
                if num_tweets > 0:
                    self.logger.info('Received {} tweets.'.format(num_tweets))

            except tweepy.error.TweepError as e:
                print(e)
                continue

            for tweet in tweets:
                if isinstance(tweet, tweepy.models.Status):
                    json_string = json.dumps(
                        tweet._json, ensure_ascii=False, indent=4)

                    producer.send(self.topic,
                                  bytes(json_string, encoding='utf-8'))

                    cur_id = int(tweet._json['id'])
                    self.last_id = max(self.last_id, cur_id)

        producer.close()


def main():
    """
    Example:
        $ python main.py -q "#pytorch" "#tensorflow" --log-level info
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--query', '-q', type=str, nargs='+')
    parser.add_argument('--log-level', type=str)
    parser.add_argument('--auth', type=str, default='auth.json')
    parser.add_argument('--lang', type=str, default='en')
    parser.add_argument(
        '--bootstrap-servers', type=str, default='192.168.99.100:32768')
    args = parser.parse_args()

    if args.log_level is not None:
        if args.log_level.lower() == 'info':
            logging.basicConfig(level=logging.INFO)

    if args.query is not None:
        tasks = [
            TwitterProducer(args.auth, args.bootstrap_servers, query, args.lang)
            for query in args.query
        ]

        for task in tasks:
            task.start()

        for task in tasks:
            task.join()


if __name__ == '__main__':
    main()
