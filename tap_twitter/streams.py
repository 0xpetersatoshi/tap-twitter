from functools import lru_cache

import singer
from singer import Transformer, metrics

from tap_twitter.client import Client, TwitterClientError
from tap_twitter.helpers import date_to_rfc3339, get_bookmark_or_max_date

LOGGER = singer.get_logger()

class TapConfigException:
    pass

class BaseStream:
    """
    A base class representing singer streams.

    :param client: The API client used extract records from the external source
    """
    tap_stream_id = None
    replication_method = None
    replication_key = None
    key_properties = []
    valid_replication_keys = []
    params = {}
    parent = None

    def __init__(self, client: Client):
        self.client = client

    def get_records(self, config: dict = None, is_parent: bool = False) -> list:
        """
        Returns a list of records for that stream.

        :param config: The tap config file
        :param is_parent: If true, may change the type of data
            that is returned for a child stream to consume
        :return: list of records
        """
        raise NotImplementedError("Child classes of BaseStream require implementation")

    def set_parameters(self, params: dict) -> None:
        """
        Sets or updates the `params` attribute of a class.

        :param params: Dictionary of parameters to set or update the class with
        """
        self.params = params

    def get_parent_data(self, start_date: str = None, config: dict = None) -> list:
        """
        Returns a list of records from the parent stream.

        :param config: The tap config file
        :return: A list of records
        """
        parent = self.parent(self.client)
        return parent.get_records(start_date, config, is_parent=True)


class IncrementalStream(BaseStream):
    """
    A child class of a base stream used to represent streams that use the
    INCREMENTAL replication method.

    :param client: The API client used extract records from the external source
    """
    replication_method = 'INCREMENTAL'
    batched = False

    def __init__(self, client):
        super().__init__(client)

    def sync(self, state: dict, stream_schema: dict, stream_metadata: dict, config: dict, transformer: Transformer) -> dict:
        """
        The sync logic for an incremental stream.

        :param state: A dictionary representing singer state
        :param stream_schema: A dictionary containing the stream schema
        :param stream_metadata: A dictionnary containing stream metadata
        :param config: A dictionary containing tap config data
        :param transformer: A singer Transformer object
        :return: State data in the form of a dictionary
        """
        start_date = singer.get_bookmark(state, self.tap_stream_id, self.replication_key, config['start_date'])
        bookmark_datetime = singer.utils.strptime_to_utc(start_date)
        max_datetime = bookmark_datetime

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(start_date, config):
                transformed_record = transformer.transform(record, stream_schema, stream_metadata)
                record_datetime = singer.utils.strptime_to_utc(transformed_record[self.replication_key])
                if record_datetime >= bookmark_datetime or self.tap_stream_id == 'users':
                    singer.write_record(self.tap_stream_id, transformed_record)
                    counter.increment()
                    max_datetime = max(record_datetime, max_datetime)

            bookmark_date = singer.utils.strftime(max_datetime)

        state = singer.write_bookmark(state, self.tap_stream_id, self.replication_key, bookmark_date)
        singer.write_state(state)
        return state


class FullTableStream(BaseStream):
    """
    A child class of a base stream used to represent streams that use the
    FULL_TABLE replication method.

    :param client: The API client used extract records from the external source
    """
    replication_method = 'FULL_TABLE'

    def __init__(self, client):
        super().__init__(client)

    def sync(self, state: dict, stream_schema: dict, stream_metadata: dict, config: dict, transformer: Transformer) -> dict:
        """
        The sync logic for an full table stream.

        :param state: A dictionary representing singer state
        :param stream_schema: A dictionary containing the stream schema
        :param stream_metadata: A dictionnary containing stream metadata
        :param config: A dictionary containing tap config data
        :param transformer: A singer Transformer object
        :return: State data in the form of a dictionary
        """
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(config):
                transformed_record = transformer.transform(record, stream_schema, stream_metadata)
                singer.write_record(self.tap_stream_id, transformed_record)
                counter.increment()

        singer.write_state(state)
        return state


class SearchTweets(IncrementalStream):
    """
    Gets recent tweets (max last 7 days) based on search query.
    """
    tap_stream_id = 'search_tweets'
    key_properties = ['id']
    replication_key = 'created_at'
    key_properties = ['id']
    valid_replication_keys = ['created_at']

    @lru_cache
    def get_records(self, start_date, config=None, is_parent=False):

        start_date = get_bookmark_or_max_date(start_date)
        start_time = date_to_rfc3339(start_date)
        tweet_fields = self.get_tweet_fields(config)
        queries = config.get("tweet_search_query")

        if not isinstance(queries, list):
            raise TapConfigException

        for query in queries:
            next_token = None
            max_results = 100
            loop = True

            LOGGER.info(f"Running query for: {query}")
            while loop:

                params = {
                    'query': query,
                    'tweet.fields': tweet_fields,
                    'start_time': start_time,
                    'next_token': next_token,
                    'max_results': max_results,
                }

                response = self.client.get_recent_tweets(params)

                meta = response.get('meta', {})
                next_token = meta.get('next_token')
                data = response.get('data')

                loop = True if next_token is not None else False

                # Throw error if there are any errors in response
                if not data and response.get('errors'):
                    errors = response.get('errors')
                    error_message = f"{errors[0].get('title')}: {errors[0].get('detail')}"

                    raise TwitterClientError(message=error_message)

                if is_parent:
                    yield (tweet['author_id'] for tweet in data)
                else:
                    yield from data

    @staticmethod
    def get_tweet_fields(config: dict) -> str:
        """Format as query string parameters the tweet fields to be returned"""
        fields = config.get('tweet_search_fields')

        # If no fields provided, return 'created_at' and 'author_id' as default
        if not fields:
            return "created_at,author_id"

        # Clean up any blank spaces
        return ",".join(map(str.strip, fields.split(",")))


class Users(IncrementalStream):
    tap_stream_id = 'users'
    key_properties = ['id']
    replication_key = 'updated_at'
    key_properties = ['id']
    valid_replication_keys = ['updated_at']
    parent = SearchTweets

    def get_records(self, start_date: str, config: dict, is_parent: bool = False) -> list:

        user_fields = self.get_user_fields(config)

        for authors in self.get_parent_data(start_date=start_date, config=config):
            author_ids = ','.join(authors)
            params = {
                'ids': author_ids,
                'user.fields': user_fields
            }

            response = self.client.get_users(params)

            data = response.get('data')

            yield from ({'updated_at': start_date, **author} for author in data)

    @staticmethod
    def get_user_fields(config: dict) -> str:
        """Format as query string parameters the user fields to be returned"""
        fields = config.get('user_fields')

        # If no fields provided, return 'created_at' and 'author_id' as default
        if not fields:
            return "created_at"

        # Clean up any blank spaces
        return ",".join(map(str.strip, fields.split(",")))


STREAMS = {
    'search_tweets': SearchTweets,
    'users': Users,
}
