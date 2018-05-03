import json
import os

import twitter


class TwitterHelper(object):

    def __init__(self, path):
        """
        Args:
            path: A path of config file.
        """
        if not os.path.exists(path):
            raise FileNotFoundError

        with open(path, 'r') as fp:
            self.config = json.load(fp)

        self.api = twitter.Api(**self.config)

    def get_search(self, **kwargs):
        statuses = self.api.GetSearch(**kwargs)

        res = []
        for status in statuses:
            if isinstance(status, twitter.models.Status):
                res.append(
                    json.dumps(status._json, indent=4, ensure_ascii=False))

        return res

    def get_trends(self):
        trends = self.api.GetTrendsCurrent()
        res = []

        for trend in trends:
            if isinstance(trend, twitter.models.Trend):
                res.append(
                    json.dumps(trend._json, indent=4, ensure_ascii=False))

        return res


def main():
    tc = TwitterHelper('config.json')
    statuses = tc.get_search(term='#Taiwan', lang='ja')
    for status in statuses:
        print(status)

    trends = tc.get_trends()
    for trend in trends:
        print(trend)


if __name__ == '__main__':
    main()
