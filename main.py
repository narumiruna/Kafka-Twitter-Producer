import argparse
import json
import logging
import os
from time import sleep

import tweepy


def read_json(file):
    with open(file, 'r') as fp:
        return json.load(fp)


class TwitterHelper(object):

    def __init__(self, auth_file):
        """
        Args:
            auth_file: A file path of auth config.
        """
        self.api = self._get_api(auth_file)
        self.last_id = 0
        self.logger = logging.getLogger('Twitter Helper')

    def _get_api(self, auth_path):
        auth_dict = read_json(auth_path)
        auth = tweepy.OAuthHandler(auth_dict['consumer_key'],
                                   auth_dict['consumer_secret'])
        auth.set_access_token(auth_dict['access_token_key'],
                              auth_dict['access_token_secret'])
        return tweepy.API(auth, wait_on_rate_limit=True)

    def run(self, query, lang=None):
        while True:
            try:
                tweets = self.api.search(
                    query, lang, since_id=self.last_id, tweet_mode='extended')
            except tweepy.error.TweepError as e:
                print(e)
                continue

            for tweet in tweets:
                if isinstance(tweet, tweepy.models.Status):
                    # print(tweet._json['full_text'])
                    print(tweet._json['created_at'])

                    cur_id = int(tweet._json['id'])
                    self.last_id = max(self.last_id, cur_id)


def main():
    """
    $ python main.py -q "#NobelPrize"
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--query', '-q', type=str)
    args = parser.parse_args()

    th = TwitterHelper('auth.json')
    th.run(args.query)


if __name__ == '__main__':
    main()
